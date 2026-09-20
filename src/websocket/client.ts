import { backOff } from "@insertish/exponential-backoff";
import { ObservableSet, observe, runInAction } from "mobx";
import WebSocket from "@insertish/isomorphic-ws";
import type { MessageEvent } from "ws";
import { Role } from "revolt-api";

import { Channel, Client } from "..";
import {
    ServerboundNotification,
    ClientboundNotification,
} from "./notifications";

/**
 * How long the serial packet queue may wait on the lookups a new `Message`
 * depends on before letting them finish in the background.
 */
const MESSAGE_DEPS_WAIT_MS = 2500;

type RaceOutcome<T> = { timedOut: false; value: T } | { timedOut: true };

/**
 * Resolve with `p`'s value, or `{ timedOut: true }` after `ms` — WITHOUT
 * cancelling `p`. Rejects if `p` rejects first.
 */
function raceTimeout<T>(p: Promise<T>, ms: number): Promise<RaceOutcome<T>> {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => resolve({ timedOut: true }), ms);
        p.then(
            (value) => {
                clearTimeout(timer);
                resolve({ timedOut: false, value });
            },
            (err) => {
                clearTimeout(timer);
                reject(err);
            },
        );
    });
}

export class WebSocketClient {
    client: Client;
    ws?: WebSocket;

    heartbeat?: number;
    connected: boolean;
    ready: boolean;

    /**
     * KIMANI: the live socket's own `Ready` said the local user is online, so
     * for as long as this socket lives NO other source may claim the local
     * user is offline (see `User.update`). The presence session exists on the
     * server the moment `Authenticate` succeeds, so an `online: false` for
     * ourselves can only be stale: a REST payload the server computed BEFORE
     * this socket authenticated but that is delivered after `Ready` (slow
     * network, retried roster sync), or a presence broadcast from another
     * session's teardown racing our creation. Cleared whenever the socket goes
     * away, so a real drop still greys everything out.
     */
    selfOnline: boolean;
    selfUserId?: string;

    ping?: number;

    constructor(client: Client) {
        this.client = client;

        this.connected = false;
        this.ready = false;
        this.selfOnline = false;
    }

    /**
     * Disconnect the WebSocket and disable heartbeats.
     */
    disconnect() {
        clearInterval(this.heartbeat);
        this.connected = false;
        this.ready = false;
        this.selfOnline = false;

        if (
            typeof this.ws !== "undefined" &&
            this.ws.readyState === WebSocket.OPEN
        ) {
            this.ws.close();
        }
    }

    /**
     * Send a notification
     * @param notification Serverbound notification
     */
    send(notification: ServerboundNotification) {
        if (
            typeof this.ws === "undefined" ||
            this.ws.readyState !== WebSocket.OPEN
        )
            return;

        const data = JSON.stringify(notification);
        if (this.client.debug) console.debug("[<] PACKET", data);
        this.ws.send(data);
    }

    /**
     * Connect the WebSocket
     * @param disallowReconnect Whether to disallow reconnection
     */
    connect(disallowReconnect?: boolean): Promise<void> {
        this.client.emit("connecting");

        return new Promise((resolve, $reject) => {
            let thrown = false;
            const reject = (err: unknown) => {
                if (!thrown) {
                    thrown = true;
                    $reject(err);
                }
            };

            this.disconnect();

            if (typeof this.client.configuration === "undefined") {
                throw new Error(
                    "Attempted to open WebSocket without syncing configuration from server.",
                );
            }

            if (typeof this.client.session === "undefined") {
                throw new Error(
                    "Attempted to open WebSocket without valid session.",
                );
            }

            const ws = new WebSocket(this.client.configuration.ws);
            this.ws = ws;

            ws.onopen = () => {
                if (typeof this.client.session === "string") {
                    this.send({
                        type: "Authenticate",
                        token: this.client.session!,
                    });
                } else {
                    this.send({
                        type: "Authenticate",
                        ...this.client.session!,
                    });
                }
            };

            const process = async (packet: ClientboundNotification) => {
                this.client.emit("packet", packet);
                try {
                    switch (packet.type) {
                        case "Bulk": {
                            for (const entry of packet.v) {
                                await process(entry);
                            }
                            break;
                        }

                        case "Error": {
                            reject(packet.error);
                            break;
                        }

                        case "Authenticated": {
                            disallowReconnect = false;
                            this.client.emit("connected");
                            this.connected = true;
                            break;
                        }

                        case "Ready": {
                            // KIMANI startup optimisation — two-phase hydration.
                            //
                            // The Revolt `Ready` packet is a full snapshot of
                            // every server, channel, user and emoji the account
                            // can see. Building a MobX observable for each entity
                            // inside a single synchronous `runInAction` blocks the
                            // main thread for seconds on large accounts, and the
                            // whole UI is gated behind it — so the user stares at a
                            // loading spinner the entire time.
                            //
                            // We hydrate in two phases instead:
                            //   Phase 1 (synchronous, tiny): the self user + every
                            //     server + every channel + the self member. This is
                            //     exactly what the app's render gate waits on
                            //     (servers/channels size), so the communities list
                            //     paints immediately.
                            //   Phase 2 (async, chunked): the bulk of users and all
                            //     emojis, hydrated in batches that yield to the
                            //     event loop between each, so avatars/names fill in
                            //     without ever freezing the main thread.
                            //
                            // Non-self members are still deferred to the lazy
                            // `server.syncMembers()` path (MemberSidebar / Members
                            // page on mount), so they are not touched here.
                            const selfUserId = packet.users.find(
                                (x) => x.relationship === "User",
                            )!._id;
                            const selfUser = packet.users.find(
                                (x) => x._id === selfUserId,
                            )!;

                            // Phase 1 — minimal synchronous hydration for paint.
                            runInAction(() => {
                                if (packet.type !== "Ready") throw 0;

                                this.client.users.createObj(selfUser);

                                for (const channel of packet.channels) {
                                    this.client.channels.createObj(channel);
                                }

                                for (const server of packet.servers) {
                                    this.client.servers.createObj(server);
                                }

                                for (const member of packet.members) {
                                    if (member._id.user === selfUserId) {
                                        this.client.members.createObj(member);
                                    }
                                }
                            });

                            this.client.user =
                                this.client.users.get(selfUserId)!;

                            // Pin our own presence to this socket (see the
                            // `selfOnline` field). Only when the server said
                            // online: an Invisible account is reported offline
                            // on purpose and must stay that way.
                            this.selfUserId = selfUserId;
                            this.selfOnline = selfUser.online === true;

                            // KIMANI diagnostics: pin down where our own dot goes
                            // grey. Logs what the packet said, what the live
                            // object holds, and EVERY later change of
                            // `user.online` with the code path that caused it.
                            // Cheap (a few lines per launch); remove once the
                            // presence problem is closed.
                            try {
                                const me = this.client.user;
                                console.info("[Presence] Ready", {
                                    packetOnline: selfUser.online,
                                    packetPresence: selfUser.status?.presence,
                                    objectOnline: me?.online,
                                    sameObject:
                                        me === this.client.users.get(selfUserId),
                                });
                                if (me) {
                                    observe(me, "online", (change: any) => {
                                        console.warn("[Presence] self.online", {
                                            from: change.oldValue,
                                            to: change.newValue,
                                            selfOnlineGuard: this.selfOnline,
                                            wsReady: this.ready,
                                            at: new Error().stack
                                                ?.split("\n")
                                                .slice(2, 7)
                                                .map((l) => l.trim().slice(0, 90)),
                                        });
                                    });
                                    [1500, 5000].forEach((ms) =>
                                        setTimeout(
                                            () =>
                                                console.info("[Presence] +" + ms + "ms", {
                                                    online: me.online,
                                                    wsReady: this.ready,
                                                }),
                                            ms,
                                        ),
                                    );
                                }
                            } catch (e) {
                                /* diagnostics must never break Ready */
                            }

                            this.client.emit("ready");
                            this.ready = true;
                            resolve();

                            // Setup heartbeat.
                            if (this.client.heartbeat > 0) {
                                this.send({ type: "Ping", data: +new Date() });
                                this.heartbeat = setInterval(() => {
                                    this.send({
                                        type: "Ping",
                                        data: +new Date(),
                                    });

                                    if (this.client.options.pongTimeout) {
                                        let pongReceived = false;

                                        this.client.once("packet", (p) => {
                                            if (p.type == "Pong")
                                                pongReceived = true;
                                        });

                                        setTimeout(() => {
                                            if (!pongReceived) {
                                                if (
                                                    this.client.options
                                                        .onPongTimeout == "EXIT"
                                                ) {
                                                    throw "Client did not receive a pong in time";
                                                } else {
                                                    console.warn(
                                                        "Warning: Client did not receive a pong in time; Reconnecting.",
                                                    );

                                                    this.disconnect();
                                                    this.connect(
                                                        disallowReconnect,
                                                    );
                                                }
                                            }
                                        }, this.client.options.pongTimeout * 1000);
                                    }
                                }, this.client.heartbeat * 1e3) as unknown as number;
                            }

                            // Phase 2 — hydrate the remaining users + emojis off
                            // the critical path, in event-loop-yielding chunks so
                            // the freshly-painted UI stays responsive. This is
                            // awaited: the WS message queue (see `ws.onmessage`
                            // below) processes packets one at a time, so awaiting
                            // here keeps ordering intact — buffered live events
                            // apply on top of a fully-hydrated snapshot rather than
                            // racing it. First paint already happened above via
                            // `resolve()`, so the await costs nothing the user sees.
                            const HYDRATE_CHUNK = 500;
                            const yieldToEventLoop = () =>
                                new Promise<void>((r) => setTimeout(r, 0));

                            const remainingUsers = packet.users.filter(
                                (u) => u._id !== selfUserId,
                            );
                            for (
                                let i = 0;
                                i < remainingUsers.length;
                                i += HYDRATE_CHUNK
                            ) {
                                const slice = remainingUsers.slice(
                                    i,
                                    i + HYDRATE_CHUNK,
                                );
                                runInAction(() => {
                                    for (const user of slice) {
                                        this.client.users.createObj(user);
                                    }
                                });
                                await yieldToEventLoop();
                            }

                            const emojis = packet.emojis ?? [];
                            for (let i = 0; i < emojis.length; i += HYDRATE_CHUNK) {
                                const slice = emojis.slice(i, i + HYDRATE_CHUNK);
                                runInAction(() => {
                                    for (const emoji of slice) {
                                        this.client.emojis.createObj(emoji);
                                    }
                                });
                                await yieldToEventLoop();
                            }

                            // Unreads depend on channels (hydrated in phase 1);
                            // sync after the bulk so it never competes with paint.
                            this.client.unreads?.sync();

                            break;
                        }

                        case "Message": {
                            if (!this.client.messages.has(packet._id)) {
                                // Resolve everything the message refers to (author,
                                // channel, server, author's member record). Each of
                                // these is a REST round-trip when the entity isn't
                                // cached yet.
                                const resolveDeps = async () => {
                                    if (
                                        packet.author ===
                                        "00000000000000000000000000"
                                    ) {
                                        if (packet.system) {
                                            switch (packet.system.type) {
                                                case "user_added":
                                                case "user_remove":
                                                    await this.client.users.fetch(
                                                        packet.system.by,
                                                    );
                                                    break;
                                                case "user_joined":
                                                    await this.client.users.fetch(
                                                        packet.system.id,
                                                    );
                                                    break;
                                                case "channel_description_changed":
                                                case "channel_icon_changed":
                                                case "channel_renamed":
                                                    await this.client.users.fetch(
                                                        packet.system.by,
                                                    );
                                                    break;
                                            }
                                        }
                                    } else {
                                        await this.client.users.fetch(
                                            packet.author,
                                        );
                                    }

                                    const channel =
                                        await this.client.channels.fetch(
                                            packet.channel,
                                        );

                                    if (channel.channel_type === "TextChannel") {
                                        const server =
                                            await this.client.servers.fetch(
                                                channel.server_id!,
                                            );
                                        if (
                                            packet.author !==
                                            "00000000000000000000000000"
                                        )
                                            await server.fetchMember(
                                                packet.author,
                                            );
                                    }

                                    return channel;
                                };

                                const commit = (channel: Channel) => {
                                    // The slow path can land after a later
                                    // delivery of the same packet.
                                    if (this.client.messages.has(packet._id))
                                        return;

                                    const message =
                                        this.client.messages.createObj(
                                            packet,
                                            true,
                                        );

                                    runInAction(() => {
                                        if (
                                            channel.channel_type ===
                                            "DirectMessage"
                                        ) {
                                            channel.active = true;
                                        }

                                        // Ids are ULIDs (lexicographically
                                        // ordered). A message finished on the
                                        // slow path must not move the channel
                                        // pointer back behind a newer one.
                                        if (
                                            !channel.last_message_id ||
                                            message._id > channel.last_message_id
                                        ) {
                                            channel.last_message_id =
                                                message._id;
                                        }

                                        if (
                                            message.author_id ===
                                            this.client.user!._id
                                        ) {
                                            // Own message echo — the author has
                                            // already seen it; keep the channel
                                            // acked so our own messages never
                                            // show an unread mark.
                                            this.client.unreads?.markRead(
                                                message.channel_id,
                                                message._id,
                                                true,
                                            );
                                        } else if (
                                            this.client.unreads &&
                                            message.mention_ids?.includes(
                                                this.client.user!._id,
                                            )
                                        ) {
                                            this.client.unreads.markMention(
                                                message.channel_id,
                                                message._id,
                                            );
                                        }
                                    });
                                };

                                // This handler runs inside the serial packet queue
                                // (see `ws.onmessage`): while it awaits, NO other
                                // packet — typing, presence, other channels — is
                                // processed. So wait for the lookups only briefly.
                                // When everything is cached (the normal case) this
                                // resolves in microtasks; if a lookup is slow, let
                                // it finish in the background instead of stalling
                                // the whole connection behind it.
                                const pending = resolveDeps();
                                const outcome = await raceTimeout(
                                    pending,
                                    MESSAGE_DEPS_WAIT_MS,
                                );
                                if (outcome.timedOut) {
                                    pending.then(commit).catch((err) =>
                                        console.warn(
                                            "[Revolt.js WS] deferred message failed",
                                            packet._id,
                                            err,
                                        ),
                                    );
                                } else {
                                    commit(outcome.value);
                                }
                            }
                            break;
                        }

                        case "MessageUpdate": {
                            const message = this.client.messages.get(packet.id);
                            if (message) {
                                message.update(packet.data);
                                this.client.emit("message/update", message);
                                this.client.emit(
                                    "message/updated",
                                    message,
                                    packet,
                                );
                            }
                            break;
                        }

                        case "MessageAppend": {
                            const message = this.client.messages.get(packet.id);
                            if (message) {
                                message.append(packet.append);
                                this.client.emit("message/append", message);
                                this.client.emit(
                                    "message/updated",
                                    message,
                                    packet,
                                );
                            }
                            break;
                        }

                        case "MessageDelete": {
                            const msg = this.client.messages.get(packet.id);
                            this.client.messages.delete(packet.id);
                            this.client.emit("message/delete", packet.id, msg);
                            break;
                        }

                        case "MessageReact": {
                            const msg = this.client.messages.get(packet.id);
                            if (msg) {
                                if (msg.reactions.has(packet.emoji_id)) {
                                    msg.reactions
                                        .get(packet.emoji_id)!
                                        .add(packet.user_id);
                                } else {
                                    msg.reactions.set(
                                        packet.emoji_id,
                                        new ObservableSet([packet.user_id]),
                                    );
                                }

                                this.client.emit(
                                    "message/updated",
                                    msg,
                                    packet,
                                );
                            }
                            break;
                        }

                        case "MessageUnreact": {
                            const msg = this.client.messages.get(packet.id);
                            if (msg) {
                                const user_ids = msg.reactions.get(
                                    packet.emoji_id,
                                );

                                if (user_ids) {
                                    user_ids.delete(packet.user_id);
                                    if (user_ids.size === 0) {
                                        msg.reactions.delete(packet.emoji_id);
                                    }
                                }

                                this.client.emit(
                                    "message/updated",
                                    msg,
                                    packet,
                                );
                            }

                            break;
                        }

                        case "MessageRemoveReaction": {
                            const msg = this.client.messages.get(packet.id);

                            if (msg) {
                                msg.reactions.delete(packet.emoji_id);

                                this.client.emit(
                                    "message/updated",
                                    msg,
                                    packet,
                                );
                            }

                            break;
                        }

                        case "BulkMessageDelete": {
                            runInAction(() => {
                                for (const id of packet.ids) {
                                    const msg = this.client.messages.get(id);
                                    this.client.messages.delete(id);
                                    this.client.emit("message/delete", id, msg);
                                }
                            });
                            break;
                        }

                        case "ChannelCreate": {
                            runInAction(async () => {
                                if (packet.type !== "ChannelCreate") throw 0;

                                if (
                                    packet.channel_type === "TextChannel" ||
                                    packet.channel_type === "VoiceChannel"
                                ) {
                                    const server =
                                        await this.client.servers.fetch(
                                            packet.server,
                                        );
                                    server.channel_ids.push(packet._id);
                                }

                                this.client.channels.createObj(packet, true);
                            });
                            break;
                        }

                        case "ChannelUpdate": {
                            const channel = this.client.channels.get(packet.id);
                            if (channel) {
                                channel.update(packet.data, packet.clear);
                                this.client.emit("channel/update", channel);
                            }
                            break;
                        }

                        case "ChannelDelete": {
                            const channel = this.client.channels.get(packet.id);
                            channel?.delete(false, true);
                            this.client.emit(
                                "channel/delete",
                                packet.id,
                                channel,
                            );
                            break;
                        }

                        case "ChannelGroupJoin": {
                            this.client.channels
                                .get(packet.id)
                                ?.updateGroupJoin(packet.user);
                            break;
                        }

                        case "ChannelGroupLeave": {
                            const channel = this.client.channels.get(packet.id);

                            if (channel) {
                                if (packet.user === this.client.user?._id) {
                                    channel.delete(false, true);
                                } else {
                                    channel.updateGroupLeave(packet.user);
                                }
                            }

                            break;
                        }

                        case "ServerCreate": {
                            runInAction(async () => {
                                const channels = [];
                                for (const channel of packet.channels) {
                                    channels.push(
                                        await this.client.channels.fetch(
                                            channel._id,
                                            channel,
                                        ),
                                    );
                                }

                                await this.client.servers.fetch(
                                    packet.id,
                                    packet.server,
                                );
                            });

                            break;
                        }

                        case "ServerUpdate": {
                            const server = this.client.servers.get(packet.id);
                            if (server) {
                                server.update(packet.data, packet.clear);
                                this.client.emit("server/update", server);
                            }
                            break;
                        }

                        case "ServerDelete": {
                            const server = this.client.servers.get(packet.id);
                            server?.delete(false, true);
                            this.client.emit(
                                "server/delete",
                                packet.id,
                                server,
                            );
                            break;
                        }

                        case "ServerMemberUpdate": {
                            const member = this.client.members.getKey(
                                packet.id,
                            );
                            if (member) {
                                member.update(packet.data, packet.clear);
                                this.client.emit("member/update", member);
                            }
                            break;
                        }

                        case "ServerMemberJoin": {
                            runInAction(async () => {
                                await this.client.servers.fetch(packet.id);
                                await this.client.users.fetch(packet.user);

                                this.client.members.createObj(
                                    {
                                        _id: {
                                            server: packet.id,
                                            user: packet.user,
                                        },
                                        joined_at: new Date().toISOString(),
                                    },
                                    true,
                                );
                            });

                            break;
                        }

                        case "ServerMemberLeave": {
                            if (packet.user === this.client.user!._id) {
                                const server_id = packet.id;
                                runInAction(() => {
                                    this.client.servers
                                        .get(server_id)
                                        ?.delete(false, true);
                                    [...this.client.members.keys()].forEach(
                                        (key) => {
                                            if (
                                                JSON.parse(key).server ===
                                                server_id
                                            ) {
                                                this.client.members.delete(key);
                                            }
                                        },
                                    );
                                });
                            } else {
                                this.client.members.deleteKey({
                                    server: packet.id,
                                    user: packet.user,
                                });
                                this.client.emit("member/leave", {
                                    server: packet.id,
                                    user: packet.user,
                                });
                            }

                            break;
                        }

                        case "ServerRoleUpdate": {
                            const server = this.client.servers.get(packet.id);
                            if (server) {
                                const role = {
                                    ...server.roles?.[packet.role_id],
                                    ...packet.data,
                                } as Role;
                                server.roles = {
                                    ...server.roles,
                                    [packet.role_id]: role,
                                };
                                this.client.emit(
                                    "role/update",
                                    packet.role_id,
                                    role,
                                    packet.id,
                                );
                            }
                            break;
                        }

                        case "ServerRoleDelete": {
                            const server = this.client.servers.get(packet.id);
                            if (server) {
                                const { [packet.role_id]: _, ...roles } =
                                    server.roles ?? {};
                                server.roles = roles;
                                this.client.emit(
                                    "role/delete",
                                    packet.role_id,
                                    packet.id,
                                );
                            }
                            break;
                        }

                        case "UserPlatformWipe": {
                            runInAction(() => {
                                const user_id = packet.user_id;

                                this.client.users.get(user_id)?.update(
                                    {
                                        username: "Removed User",
                                        online: false,
                                        relationship: "None",
                                        flags: packet.flags,
                                    },
                                    [
                                        "Avatar",
                                        "ProfileBackground",
                                        "ProfileContent",
                                        "StatusPresence",
                                        "StatusText",
                                    ],
                                );

                                const dm_channel = [
                                    ...this.client.channels.values(),
                                ].find(
                                    (channel) =>
                                        channel.channel_type ===
                                            "DirectMessage" &&
                                        channel.recipient_ids?.includes(
                                            user_id,
                                        ),
                                );

                                if (dm_channel) {
                                    this.client.channels.delete(dm_channel._id);
                                }

                                const member_ids = [
                                    ...this.client.members.values(),
                                ]
                                    .filter(
                                        (member) => member._id.user === user_id,
                                    )
                                    .map((member) => member._id);

                                for (const member_id of member_ids) {
                                    this.client.members.deleteKey(member_id);
                                }

                                for (const message of [
                                    ...this.client.messages.values(),
                                ].filter(
                                    (message) => message.author_id === user_id,
                                )) {
                                    message.content = "(message withheld)";
                                    message.attachments = [];
                                    message.embeds = [];
                                }
                            });
                            break;
                        }

                        case "UserUpdate": {
                            this.client.users
                                .get(packet.id)
                                ?.update(packet.data, packet.clear);
                            break;
                        }

                        case "UserRelationship": {
                            const user = this.client.users.get(packet.user._id);
                            if (user) {
                                user.update({
                                    ...packet.user,
                                    relationship: packet.status,
                                });
                            } else {
                                this.client.users.createObj({
                                    ...packet.user,
                                    relationship: packet.status,
                                });
                            }

                            break;
                        }

                        case "ChannelStartTyping": {
                            const channel = this.client.channels.get(packet.id);
                            const user = packet.user;

                            if (channel) {
                                channel.updateStartTyping(user);

                                clearInterval(timeouts[packet.id + user]);
                                timeouts[packet.id + user] = setTimeout(() => {
                                    channel!.updateStopTyping(user);
                                }, 3000) as unknown as number;
                            }

                            break;
                        }

                        case "ChannelStopTyping": {
                            this.client.channels
                                .get(packet.id)
                                ?.updateStopTyping(packet.user);
                            clearInterval(timeouts[packet.id + packet.user]);
                            break;
                        }

                        case "ChannelAck": {
                            this.client.unreads?.markRead(
                                packet.id,
                                packet.message_id,
                            );
                            break;
                        }

                        case "EmojiCreate": {
                            this.client.emojis.createObj(packet, true);
                            break;
                        }

                        case "EmojiDelete": {
                            const emoji = this.client.emojis.get(packet.id);
                            this.client.emit("emoji/delete", packet.id, emoji);
                            break;
                        }

                        case "Pong": {
                            this.ping = +new Date() - packet.data;
                            break;
                        }

                        default:
                            this.client.debug &&
                                console.warn(
                                    `Warning: Unhandled packet! ${packet.type}`,
                                );
                    }
                } catch (e) {
                    console.error(e);
                }
            };

            const timeouts: Record<string, number> = {};
            const handle = async (msg: WebSocket.MessageEvent) => {
                let data: any = msg.data;

                // Defensive: Capacitor Android WebView occasionally delivers
                // larger text WS frames as Blob instead of string (observed
                // for the Revolt `Ready` snapshot — many KB of JSON containing
                // users/channels/servers/members/emojis). The original
                // `typeof data !== "string"` check silently dropped these,
                // leaving the client with only `Authenticated` and never
                // populating any of the collections — visible to users as
                // "WS connected but home is empty / serverCount=0" while
                // the exact same account loads fine in a desktop browser.
                // Decode Blob/ArrayBuffer/typed-array to string in-place so
                // the downstream JSON.parse + process(packet) path works
                // regardless of how the WebView surfaces the frame.
                try {
                    if (typeof data !== "string") {
                        if (typeof Blob !== "undefined" && data instanceof Blob) {
                            data = await data.text();
                        } else if (data instanceof ArrayBuffer) {
                            data = new TextDecoder("utf-8").decode(
                                new Uint8Array(data),
                            );
                        } else if (
                            ArrayBuffer.isView(data) &&
                            !(data instanceof DataView)
                        ) {
                            data = new TextDecoder("utf-8").decode(
                                data as ArrayBufferView,
                            );
                        }
                    }
                } catch (decodeErr) {
                    console.warn(
                        "[Revolt.js WS] failed to decode non-string frame",
                        decodeErr,
                    );
                    return;
                }

                if (typeof data !== "string") {
                    console.warn(
                        "[Revolt.js WS] dropping frame of unsupported type",
                        Object.prototype.toString.call(data),
                    );
                    return;
                }

                if (this.client.debug) console.debug("[>] PACKET", data);
                const packet = JSON.parse(data) as ClientboundNotification;
                await process(packet);
            };

            let processing = false;
            const queue: WebSocket.MessageEvent[] = [];
            ws.onmessage = async (data: MessageEvent) => {
                queue.push(data);

                if (!processing) {
                    processing = true;
                    while (queue.length > 0) {
                        await handle(queue.shift()!);
                    }
                    processing = false;
                }
            };

            ws.onerror = (err: any) => {
                reject(err);
            };

            ws.onclose = () => {
                this.client.emit("dropped");

                Object.keys(timeouts)
                    .map((k) => timeouts[k])
                    .forEach(clearTimeout);

                // KIMANI: a STALE socket's late `onclose` (connect() orphans a
                // CONNECTING socket, and `disconnect()` only closes an OPEN
                // one) must not touch shared state. This handler used to run
                // unconditionally, so after an offline launch → network back →
                // new socket Ready, the orphan's late close marked EVERY user
                // (self included) offline again and cleared `ready`/`connected`
                // for the live socket. Presence then stayed wrong until some
                // screen re-fetched the roster (e.g. switching tabs).
                if (this.ws !== ws) return;

                this.connected = false;
                this.ready = false;
                this.selfOnline = false;

                runInAction(() => {
                    [...this.client.users.values()].forEach(
                        (user) => (user.online = false),
                    );
                    [...this.client.channels.values()].forEach((channel) =>
                        channel.typing_ids.clear(),
                    );
                });

                if (!disallowReconnect && this.client.autoReconnect) {
                    backOff(() => this.connect(true)).catch(reject);
                }
            };
        });
    }
}
