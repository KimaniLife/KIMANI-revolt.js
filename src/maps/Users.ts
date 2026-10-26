import type {
    BotInformation,
    UserStatus,
    User as ApiUser,
    RelationshipStatus,
    FieldsUser,
    DataEditUser,
} from "revolt-api";
import type { File } from "revolt-api";

import {
    makeAutoObservable,
    action,
    runInAction,
    computed,
    observable,
} from "mobx";
import isEqual from "lodash.isequal";

import { U32_MAX, UserFlags, UserPermission } from "../permissions/definitions";
import { toNullable, Nullable } from "../util/null";
import Collection from "./Collection";
import { Client, FileArgs } from "..";
import _ from "lodash";
import { decodeTime } from "ulid";

type UserI = ApiUser & { relationship_note?: string | null };

export class User {
    client: Client;

    _id: string;
    username: string;

    avatar: Nullable<File>;
    badges: Nullable<number>;
    status: Nullable<UserStatus>;
    relationship: Nullable<RelationshipStatus>;
    /** Optional note visible only to the Incoming recipient. */
    relationship_note: Nullable<string>;
    online: boolean;
    privileged: boolean;
    flags: Nullable<number>;
    bot: Nullable<BotInformation>;

    /**
     * Get timestamp when this user was created.
     */
    get createdAt() {
        return decodeTime(this._id);
    }

    constructor(client: Client, data: UserI) {
        this.client = client;

        this._id = data._id;
        this.username = data.username;

        this.avatar = toNullable(data.avatar);
        this.badges = toNullable(data.badges);
        this.status = toNullable(data.status);
        this.relationship = toNullable(data.relationship);
        this.relationship_note = toNullable(
            data.relationship_note,
        );
        this.online = data.online ?? false;
        this.privileged = data.privileged ?? false;
        this.flags = toNullable(data.flags);
        this.bot = toNullable(data.bot);

        makeAutoObservable(this, {
            _id: false,
            client: false,
        });
    }

    @action update(data: Partial<UserI>, clear: FieldsUser[] = []) {
        const apply = (key: string) => {
            // This code has been tested.
            if (
                // @ts-expect-error TODO: clean up types here
                typeof data[key] !== "undefined" &&
                // @ts-expect-error TODO: clean up types here
                !isEqual(this[key], data[key])
            ) {
                // @ts-expect-error TODO: clean up types here
                this[key] = data[key];

                if (key === "relationship") {
                    this.client.emit("user/relationship", this);
                }
            }
        };

        for (const entry of clear) {
            switch (entry) {
                case "Avatar":
                    this.avatar = null;
                    break;
                case "StatusText": {
                    if (this.status) {
                        this.status.text = undefined;
                    }
                    break;
                }
                // Leaving Invisible clears the presence; ignoring it kept the
                // member "Invisible" (shown offline) while they were online.
                case "StatusPresence": {
                    if (this.status) {
                        this.status.presence = undefined;
                    }
                    break;
                }
            }
        }

        apply("username");
        apply("avatar");
        apply("badges");
        apply("status");
        // Note must land before relationship: the "relationship" apply below
        // fires the `user/relationship` event synchronously, and listeners
        // (e.g. the pending-requests list) read `relationship_note` off this
        // same object when they re-render in response to it.
        // Only a move away from Incoming drops the note. An Incoming update
        // without one is a local rollback (e.g. a failed accept) and must keep
        // it; a genuinely new request always passes through None/Outgoing first.
        if (typeof data.relationship !== "undefined" &&
            data.relationship !== "Incoming") {
            this.relationship_note = null;
        }
        apply("relationship_note");
        apply("relationship");
        // Our own presence belongs to our live socket: while it is authenticated
        // and its Ready said we are online, an `online: false` about ourselves
        // is stale by construction (a REST payload computed before the socket
        // authenticated but delivered after Ready, a teardown broadcast from a
        // previous session) and must not grey out our own dot. See
        // `WebSocketClient.selfOnline`.
        const ws = this.client.websocket;
        if (
            !(
                data.online === false &&
                ws?.selfOnline &&
                ws.selfUserId === this._id
            )
        ) {
            apply("online");
        }
        apply("privileged");
        apply("flags");
        apply("bot");
    }

    /**
     * Open a DM with a user
     * @returns DM Channel
     */
    async openDM() {
        let dm = [...this.client.channels.values()].find(
            (x) => x.channel_type === "DirectMessage" && x.recipient == this,
        );

        if (!dm) {
            const data = await this.client.api.get(
                `/users/${this._id as ""}/dm`,
            );
            dm = await this.client.channels.fetch(data._id, data)!;
        }

        runInAction(() => {
            dm!.active = true;
        });

        return dm;
    }

    /**
     * Send a friend request to a user
     * @param note Optional note shown to the recipient alongside the request
     */
    async addFriend(note?: string) {
        const request: { username: string; note?: string } = { username: this._id, note };
        return await this.client.api.post(`/users/friend`, request);
    }

    /**
     * Remove a user from the friend list
     */
    async removeFriend() {
        return await this.client.api.delete(`/users/${this._id as ""}/friend`);
    }

    /**
     * Block a user
     */
    async blockUser() {
        return await this.client.api.put(`/users/${this._id as ""}/block`);
    }

    /**
     * Unblock a user
     */
    async unblockUser() {
        return await this.client.api.delete(`/users/${this._id as ""}/block`);
    }

    /**
     * Fetch the profile of a user
     * @returns The profile of the user
     */
    async fetchProfile() {
        return await this.client.api.get(`/users/${this._id as ""}/profile`);
    }

    /**
     * Fetch the mutual connections of the current user and a target user
     * @returns The mutual connections of the current user and a target user
     */
    async fetchMutual() {
        return await this.client.api.get(`/users/${this._id as ""}/mutual`);
    }

    /**
     * Get the default avatar URL of a user
     */
    get defaultAvatarURL() {
        return `${this.client.apiURL}/users/${this._id}/default_avatar`;
    }

    /**
     * Get a pre-configured avatar URL of a user
     */
    get avatarURL() {
        return this.generateAvatarURL({ max_side: 256 });
    }

    /**
     * Get a pre-configured animated avatar URL of a user
     */
    /**
     * Frozen by an admin (`UserFlags.Restricted`): the member keeps their
     * login and profile but may only write in staff DMs.
     */
    get restricted() {
        return ((this.flags ?? 0) & UserFlags.Restricted) !== 0;
    }

    get animatedAvatarURL() {
        return this.generateAvatarURL({ max_side: 256 }, true);
    }

    @computed generateAvatarURL(...args: FileArgs) {
        return (
            this.client.generateFileURL(this.avatar ?? undefined, ...args) ??
            this.defaultAvatarURL
        );
    }

    @computed get permission() {
        // A privileged viewer (Auth0 `revolt:privileged`) may reach any
        // member without a friendship; the server allows the same.
        if (this.client.user?.privileged) return U32_MAX;

        let permissions = 0;
        switch (this.relationship) {
            case "Friend":
            case "User":
                return U32_MAX;
            case "Blocked":
            case "BlockedOther":
                return UserPermission.Access;
            case "Incoming":
            case "Outgoing":
                permissions = UserPermission.Access;
        }

        if (
            [...this.client.channels.values()].find(
                (channel) =>
                    (channel.channel_type === "Group" ||
                        channel.channel_type === "DirectMessage") &&
                    channel.recipient_ids?.includes(this.client.user!._id),
            ) ||
            [...this.client.members.values()].find(
                (member) => member._id.user === this.client.user!._id,
            )
        ) {
            if (this.client.user?.bot || this.bot) {
                permissions |= UserPermission.SendMessage;
            }

            permissions |= UserPermission.Access | UserPermission.ViewProfile;
        }

        return permissions;
    }
}

export default class Users extends Collection<string, User> {
    /**
     * Ids this account is known not to see: `GET /users/:id` answered 404
     * (the server hides a restricted member from non-staff), or a DM peer the
     * Ready snapshot left out. Lets the app say "unavailable" only when that
     * is known, instead of while users are still loading.
     */
    readonly unavailable = observable.set<string>();

    constructor(client: Client) {
        super(client);
        this.createObj = this.createObj.bind(this);
        this.createObjFromRest = this.createObjFromRest.bind(this);
        this.set(
            "00000000000000000000000000",
            new User(client, {
                _id: "00000000000000000000000000",
                username: "Revolt",
            }),
        );
    }

    @action $get(id: string, data?: UserI) {
        const user = this.get(id)!;
        if (data) user.update(data);
        return user;
    }

    /**
     * Fetch a user
     * @param id User ID
     * @returns User
     */
    async fetch(id: string, data?: UserI) {
        if (this.has(id)) return this.$get(id, data);
        if (data) return this.createObj(data);

        try {
            return this.createObj(await this.client.api.get(`/users/${id as ""}`));
        } catch (err) {
            if ((err as { response?: { status?: number } })?.response?.status === 404) {
                runInAction(() => this.unavailable.add(id));
            }
            throw err;
        }
    }

    /**
     * Create a user object.
     * This is meant for internal use only.
     * @param data: User Data
     * @returns User
     */
    createObj(data: UserI) {
        if (this.has(data._id)) return this.$get(data._id, data);
        const user = new User(this.client, data);

        runInAction(() => {
            this.set(data._id, user);
            this.unavailable.delete(data._id);
        });

        this.client.emit("user/relationship", user);
        return user;
    }

    /**
     * Create or update a user from a REST payload (rosters, messages with
     * users). Its `online` was computed when the server built the response, so
     * while our socket is live an `online: false` in it can be older than the
     * `UserUpdate { online: true }` the socket already delivered: a roster in
     * flight (or applied in chunks) used to grey out a member who had just come
     * online until their next presence change (Oct 2026). A real disconnect
     * still arrives as a socket `UserUpdate`, and a socket close marks everyone
     * offline first, so any live `true` dates from the current connection.
     * @param data User data from a REST response
     * @returns User
     */
    createObjFromRest(data: UserI) {
        const live = this.get(data._id);
        const stale =
            data.online === false && live?.online && this.client.websocket?.ready;
        return this.createObj(stale ? { ...data, online: true } : data);
    }

    /**
     * Edit the current user
     * @param data User edit data object
     */
    async edit(data: DataEditUser) {
        await this.client.api.patch("/users/@me", data);
    }

    /**
     * Change the username of the current user
     * @param username New username
     * @param password Current password
     */
    async changeUsername(username: string, password: string) {
        return await this.client.api.patch("/users/@me/username", {
            username,
            password,
        });
    }
}
