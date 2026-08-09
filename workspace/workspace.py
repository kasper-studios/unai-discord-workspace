"""UnAI Discord Workspace implementation.

Subclasses Workspace from UnAI SDK. Interacts natively with Discord REST API v10
using user or bot tokens. Provides full capability for reading servers, channels,
messages with attachments, sending messages, replies, member lookup, and notifications.

Follows ADR-0004 for one-shot login tool state management.
"""

import base64
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import aiohttp

from unai.sdk import Workspace, tool

DISCORD_API_BASE = "https://discord.com/api/v10"


def _file_to_base64_data_uri(file_path: str) -> str:
    p = Path(file_path)
    if not p.exists():
        raise RuntimeError(f"Avatar/Banner file not found: {file_path}")
    data = p.read_bytes()
    b64 = base64.b64encode(data).decode("utf-8")
    ext = p.suffix.lower()
    mime = "image/png"
    if ext in [".jpg", ".jpeg"]:
        mime = "image/jpeg"
    elif ext == ".gif":
        mime = "image/gif"
    elif ext == ".webp":
        mime = "image/webp"
    return f"data:{mime};base64,{b64}"


def _get_data_dir() -> Path:
    d = Path.home() / ".unai" / "data" / "discord"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _get_token_file() -> Path:
    return _get_data_dir() / "session.json"


class DiscordWorkspace(Workspace):
    """Native Discord Workspace for autonomous AI agents."""

    def __init__(self, runtime_id: str = "discord", bus: Optional[Any] = None, **kwargs: Any):
        super().__init__(runtime_id=runtime_id, bus=bus, **kwargs)
        self._token: Optional[str] = None
        self._user_info: Optional[Dict[str, Any]] = None
        self._gateway_ws: Optional[Any] = None
        self._gateway_task: Optional[Any] = None
        self._gateway_connected: bool = False
        self._gateway_last_seq: Optional[int] = None
        self._notifications_cache: List[Dict[str, Any]] = []
        self._current_presence: Optional[Dict[str, Any]] = None
        self._load_saved_token()
        self._load_notifications_cache()

    def _build_presence_payload(self) -> Dict[str, Any]:
        p = self._current_presence or {}
        status = p.get("status", "online")
        act_type_str = p.get("activity_type", "custom").lower()
        act_name = p.get("activity_name", "")
        emoji_name = p.get("emoji", "")

        type_map = {
            "playing": 0,
            "streaming": 1,
            "listening": 2,
            "watching": 3,
            "custom": 4,
            "competing": 5,
        }
        act_type = type_map.get(act_type_str, 4)

        activities = []
        if act_name or emoji_name or act_type_str == "custom":
            act_obj: Dict[str, Any] = {
                "name": "Custom Status" if act_type == 4 else act_name,
                "type": act_type,
            }
            if act_type == 4:
                if act_name:
                    act_obj["state"] = act_name
                if emoji_name:
                    act_obj["emoji"] = {"name": emoji_name}
            else:
                if act_name:
                    act_obj["state"] = act_name

            activities.append(act_obj)

        return {
            "since": None,
            "activities": activities,
            "status": status,
            "afk": status == "idle",
        }

    def _load_notifications_cache(self) -> None:
        nf = _get_data_dir() / "notifications_cache.json"
        if nf.exists():
            try:
                self._notifications_cache = json.loads(nf.read_text())
            except Exception:
                self._notifications_cache = []

    def _save_notifications_cache(self) -> None:
        nf = _get_data_dir() / "notifications_cache.json"
        try:
            # Keep max 200 notifications in persistent cache
            nf.write_text(json.dumps(self._notifications_cache[:200], ensure_ascii=False, indent=2))
        except Exception:
            pass

    def _add_notification(self, notif: Dict[str, Any]) -> None:
        self._notifications_cache.insert(0, notif)
        self._save_notifications_cache()
        if self.bus:
            try:
                self.bus.emit("discord.notification", notif)
            except Exception:
                pass

    def _load_saved_token(self) -> None:
        tf = _get_token_file()
        if tf.exists():
            try:
                data = json.loads(tf.read_text())
                self._token = data.get("token")
                self._user_info = data.get("user")
            except Exception:
                self._token = None

    @property
    def is_logged_in(self) -> bool:
        return bool(self._token)

    def _get_headers(self) -> Dict[str, str]:
        if not self._token:
            raise RuntimeError("Discord token is not set. Please call discord.login(token) first.")
        token = self._token.strip()
        auth_header = token if token.startswith("Bot ") or token.startswith("Bearer ") else token
        return {
            "Authorization": auth_header,
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 UnAI-Discord/1.0",
        }

    async def _api_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
    ) -> Any:
        url = f"{DISCORD_API_BASE}{endpoint}"
        headers = self._get_headers()
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, url, headers=headers, params=params, json=json_data, data=data
            ) as response:
                if response.status == 204:
                    return None
                resp_text = await response.text()
                if response.status >= 400:
                    try:
                        err_json = json.loads(resp_text)
                        msg = err_json.get("message", resp_text)
                    except Exception:
                        msg = resp_text
                    raise RuntimeError(f"Discord API error ({response.status}): {msg}")
                try:
                    return json.loads(resp_text)
                except Exception:
                    return resp_text

    async def _gateway_listener(self) -> None:
        """Background Gateway listener loop receiving live events from Discord Gateway v10."""
        import datetime
        ws_url = "wss://gateway.discord.gg/?v=10&encoding=json"
        token = self._token.strip() if self._token else ""
        if not token:
            self._gateway_connected = False
            return

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        self._gateway_ws = ws
                        self._gateway_connected = True
                        heartbeat_task = None

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                op = data.get("op")
                                t = data.get("t")
                                d = data.get("d", {})
                                s = data.get("s")
                                if s is not None:
                                    self._gateway_last_seq = s

                                # OP 10 Hello -> Send Identify and start heartbeat
                                if op == 10:
                                    interval_ms = d.get("heartbeat_interval", 41250)

                                    async def heartbeat_loop(interval: float):
                                        while True:
                                            await asyncio.sleep(interval)
                                            try:
                                                await ws.send_json({"op": 1, "d": self._gateway_last_seq})
                                            except Exception:
                                                break

                                    heartbeat_task = asyncio.create_task(heartbeat_loop(interval_ms / 1000.0))

                                    identify_payload = {
                                        "op": 2,
                                        "d": {
                                            "token": token,
                                            "intents": 3276799,
                                            "presence": self._build_presence_payload(),
                                            "properties": {
                                                "os": "linux",
                                                "browser": "UnAI-Discord",
                                                "device": "UnAI-Discord"
                                            }
                                        }
                                    }
                                    await ws.send_json(identify_payload)

                                # OP 1 Heartbeat Request from Gateway -> Respond immediately
                                elif op == 1:
                                    await ws.send_json({"op": 1, "d": self._gateway_last_seq})

                                # OP 0 Dispatch Event
                                elif op == 0:
                                    my_id = self._user_info.get("id") if self._user_info else None

                                    if t == "MESSAGE_CREATE":
                                        author = d.get("author", {})
                                        author_id = author.get("id")
                                        if my_id and author_id == my_id:
                                            continue  # Ignore self messages

                                        guild_id = d.get("guild_id")
                                        mentions = d.get("mentions", [])
                                        is_dm = guild_id is None
                                        is_mentioned = any(m.get("id") == my_id for m in mentions) if my_id else False

                                        notif_type = "dm" if is_dm else ("mention" if is_mentioned else "message")
                                        notif = {
                                            "id": d.get("id"),
                                            "type": notif_type,
                                            "channel_id": d.get("channel_id"),
                                            "guild_id": guild_id,
                                            "author": author.get("username"),
                                            "author_global_name": author.get("global_name"),
                                            "author_id": author_id,
                                            "content": d.get("content", ""),
                                            "timestamp": d.get("timestamp"),
                                            "attachments_count": len(d.get("attachments", [])),
                                            "read": False,
                                        }
                                        self._add_notification(notif)

                                    elif t == "VOICE_STATE_UPDATE":
                                        user_id = d.get("user_id")
                                        if my_id and user_id == my_id:
                                            continue
                                        channel_id = d.get("channel_id")
                                        member = d.get("member", {}).get("user", {})
                                        notif = {
                                            "id": f"voice_{user_id}_{channel_id}_{datetime.datetime.now().timestamp()}",
                                            "type": "voice_state",
                                            "user_id": user_id,
                                            "username": member.get("username"),
                                            "channel_id": channel_id,
                                            "guild_id": d.get("guild_id"),
                                            "mute": d.get("mute", False),
                                            "deaf": d.get("deaf", False),
                                            "self_mute": d.get("self_mute", False),
                                            "self_deaf": d.get("self_deaf", False),
                                            "timestamp": datetime.datetime.now().isoformat(),
                                            "read": False,
                                        }
                                        self._add_notification(notif)

                                    elif t == "MESSAGE_REACTION_ADD":
                                        user_id = d.get("user_id")
                                        if my_id and user_id == my_id:
                                            continue
                                        emoji = d.get("emoji", {}).get("name")
                                        notif = {
                                            "id": f"react_{user_id}_{d.get('message_id')}_{emoji}",
                                            "type": "reaction",
                                            "user_id": user_id,
                                            "channel_id": d.get("channel_id"),
                                            "guild_id": d.get("guild_id"),
                                            "message_id": d.get("message_id"),
                                            "emoji": emoji,
                                            "timestamp": datetime.datetime.now().isoformat(),
                                            "read": False,
                                        }
                                        self._add_notification(notif)

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                        if heartbeat_task:
                            heartbeat_task.cancel()

            except Exception:
                pass
            finally:
                self._gateway_connected = False
                self._gateway_ws = None
                await asyncio.sleep(5.0)  # Reconnect delay

    # ====================================================================
    # Auth Tools (ADR-0004)
    # ====================================================================

    @tool(
        "discord.login",
        description="Login to Discord account or bot using a token",
        arguments={
            "token": {"type": "string", "description": "Discord user token or Bot token"}
        },
        enabled_if=lambda ws: not ws.is_logged_in,
    )
    async def login(self, token: str, reason: Optional[str] = None) -> str:
        clean_token = token.strip()
        auth_header = clean_token if clean_token.startswith("Bot ") or clean_token.startswith("Bearer ") else clean_token
        headers = {
            "Authorization": auth_header,
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 UnAI-Discord/1.0",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{DISCORD_API_BASE}/users/@me", headers=headers) as resp:
                if resp.status >= 400:
                    txt = await resp.text()
                    raise RuntimeError(f"Failed to authenticate Discord token ({resp.status}): {txt}")
                user_info = await resp.json()

        self._token = clean_token
        self._user_info = user_info
        tf = _get_token_file()
        tf.write_text(json.dumps({"token": clean_token, "user": user_info}))

        username = user_info.get("username", "user")
        discrim = user_info.get("discriminator", "0")
        tag = username if discrim == "0" else f"{username}#{discrim}"
        return f"Successfully logged into Discord as {tag} (ID: {user_info.get('id')})"

    @tool(
        "discord.logout",
        description="Logout from Discord account and clear saved token",
        enabled_if=lambda ws: ws.is_logged_in,
    )
    async def logout(self, reason: Optional[str] = None) -> str:
        self._token = None
        self._user_info = None
        tf = _get_token_file()
        if tf.exists():
            tf.unlink()
        return "Logged out from Discord successfully."

    # ====================================================================
    # Core Discord Tools
    # ====================================================================

    @tool(
        "discord.status",
        description="Get connection status and current logged in Discord user profile",
    )
    async def status(self, reason: Optional[str] = None) -> Dict[str, Any]:
        if not self._token:
            return {"connected": False, "user": None, "info": "Not logged in. Call discord.login(token)"}
        try:
            me = await self._api_request("GET", "/users/@me")
            self._user_info = me
            return {
                "connected": True,
                "user": {
                    "id": me.get("id"),
                    "username": me.get("username"),
                    "global_name": me.get("global_name"),
                    "bot": me.get("bot", False),
                    "email": me.get("email"),
                    "avatar": me.get("avatar"),
                },
                "info": "Connected to Discord API",
            }
        except Exception as e:
            return {"connected": False, "user": None, "error": str(e)}

    @tool(
        "discord.servers.list",
        description="List joined Discord servers (guilds) for the account",
    )
    async def servers_list(self, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        guilds = await self._api_request("GET", "/users/@me/guilds")
        out = []
        for g in guilds:
            out.append({
                "id": g.get("id"),
                "name": g.get("name"),
                "owner": g.get("owner", False),
                "permissions": g.get("permissions"),
                "icon": g.get("icon"),
            })
        return out

    @tool(
        "discord.servers.my_member",
        description="Get your own member profile, nickname, roles, joined_at date, and permissions on a specific server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def servers_my_member(self, guild_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        member = await self._api_request("GET", f"/guilds/{guild_id}/members/@me")
        user = member.get("user", {})
        return {
            "guild_id": guild_id,
            "id": user.get("id"),
            "username": user.get("username"),
            "global_name": user.get("global_name"),
            "nickname": member.get("nick"),
            "roles": member.get("roles", []),
            "joined_at": member.get("joined_at"),
            "premium_since": member.get("premium_since"),
            "permissions": member.get("permissions"),
            "avatar": member.get("avatar"),
            "deaf": member.get("deaf", False),
            "mute": member.get("mute", False),
            "pending": member.get("pending", False),
        }

    @tool(
        "discord.servers.get",
        description="Get detailed information about a Discord server (guild) including owner, description, roles, icon, and member counts",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"}
        },
    )
    async def servers_get(self, guild_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        g = await self._api_request("GET", f"/guilds/{guild_id}?with_counts=true")
        roles = [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "color": r.get("color"),
                "position": r.get("position"),
                "permissions": r.get("permissions"),
            }
            for r in g.get("roles", [])
        ]
        return {
            "id": g.get("id"),
            "name": g.get("name"),
            "description": g.get("description"),
            "owner_id": g.get("owner_id"),
            "icon": g.get("icon"),
            "banner": g.get("banner"),
            "splash": g.get("splash"),
            "approximate_member_count": g.get("approximate_member_count"),
            "approximate_presence_count": g.get("approximate_presence_count"),
            "preferred_locale": g.get("preferred_locale"),
            "system_channel_id": g.get("system_channel_id"),
            "afk_channel_id": g.get("afk_channel_id"),
            "afk_timeout": g.get("afk_timeout"),
            "roles": roles,
        }

    @tool(
        "discord.servers.update",
        description="Update server (guild) settings such as name, description, icon, banner, system_channel_id, or afk settings (requires MANAGE_GUILD permission)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "name": {"type": "string", "description": "New server name", "default": ""},
            "description": {"type": "string", "description": "New server description", "default": ""},
            "icon_path": {"type": "string", "description": "Local image file path to upload as server icon", "default": ""},
            "banner_path": {"type": "string", "description": "Local image file path to upload as server banner", "default": ""},
            "system_channel_id": {"type": "string", "description": "Channel ID for system messages", "default": ""},
            "afk_channel_id": {"type": "string", "description": "Voice channel ID for AFK users", "default": ""},
            "afk_timeout": {"type": "integer", "description": "AFK timeout in seconds (60, 300, 900, 1800, 3600)", "default": 0}
        },
    )
    async def servers_update(
        self,
        guild_id: str,
        name: str = "",
        description: str = "",
        icon_path: str = "",
        banner_path: str = "",
        system_channel_id: str = "",
        afk_channel_id: str = "",
        afk_timeout: int = 0,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if name:
            payload["name"] = name
        if description:
            payload["description"] = description
        if icon_path:
            payload["icon"] = _file_to_base64_data_uri(icon_path)
        if banner_path:
            payload["banner"] = _file_to_base64_data_uri(banner_path)
        if system_channel_id:
            payload["system_channel_id"] = system_channel_id
        if afk_channel_id:
            payload["afk_channel_id"] = afk_channel_id
        if afk_timeout > 0:
            payload["afk_timeout"] = afk_timeout

        if not payload:
            raise RuntimeError("At least one setting parameter must be specified to update server.")

        g = await self._api_request("PATCH", f"/guilds/{guild_id}", json_data=payload)
        return {
            "id": g.get("id"),
            "name": g.get("name"),
            "description": g.get("description"),
            "icon": g.get("icon"),
            "banner": g.get("banner"),
            "info": "Server settings updated successfully.",
        }

    @tool(
        "discord.channels.create",
        description="Create a new text, voice, or category channel in a server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID"},
            "name": {"type": "string", "description": "Channel name"},
            "type": {"type": "string", "description": "Channel type: 'text', 'voice', 'category', 'news', 'forum'", "default": "text"},
            "topic": {"type": "string", "description": "Optional channel topic", "default": ""},
            "parent_id": {"type": "string", "description": "Optional category ID to place channel in", "default": ""},
            "nsfw": {"type": "boolean", "description": "Whether channel is NSFW", "default": False}
        },
    )
    async def channels_create(
        self,
        guild_id: str,
        name: str,
        type: str = "text",
        topic: str = "",
        parent_id: str = "",
        nsfw: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        type_str_map = {"text": 0, "voice": 2, "category": 4, "news": 5, "forum": 15}
        ctype = type_str_map.get(type.lower(), 0)

        payload: Dict[str, Any] = {"name": name, "type": ctype}
        if topic:
            payload["topic"] = topic
        if parent_id:
            payload["parent_id"] = parent_id
        if nsfw:
            payload["nsfw"] = nsfw

        c = await self._api_request("POST", f"/guilds/{guild_id}/channels", json_data=payload)
        return {
            "id": c.get("id"),
            "name": c.get("name"),
            "type": type,
            "guild_id": c.get("guild_id"),
            "parent_id": c.get("parent_id"),
            "info": f"Channel '{name}' created successfully in guild '{guild_id}'.",
        }

    @tool(
        "discord.channels.update",
        description="Update channel properties (name, topic, parent_id/category, position, or nsfw)",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID to update"},
            "name": {"type": "string", "description": "New channel name", "default": ""},
            "topic": {"type": "string", "description": "New channel topic", "default": ""},
            "parent_id": {"type": "string", "description": "New category ID", "default": ""},
            "position": {"type": "integer", "description": "New position integer", "default": -1},
            "nsfw": {"type": "boolean", "description": "Set NSFW flag", "default": False}
        },
    )
    async def channels_update(
        self,
        channel_id: str,
        name: str = "",
        topic: str = "",
        parent_id: str = "",
        position: int = -1,
        nsfw: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if name:
            payload["name"] = name
        if topic:
            payload["topic"] = topic
        if parent_id:
            payload["parent_id"] = parent_id
        if position >= 0:
            payload["position"] = position
        if nsfw:
            payload["nsfw"] = nsfw

        if not payload:
            raise RuntimeError("At least one parameter must be specified to update channel.")

        c = await self._api_request("PATCH", f"/channels/{channel_id}", json_data=payload)
        return {
            "id": c.get("id"),
            "name": c.get("name"),
            "topic": c.get("topic"),
            "parent_id": c.get("parent_id"),
            "info": f"Channel '{channel_id}' updated successfully.",
        }

    @tool(
        "discord.channels.delete",
        description="Delete a channel or category from a Discord server",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID to delete"}
        },
    )
    async def channels_delete(self, channel_id: str, reason: Optional[str] = None) -> str:
        await self._api_request("DELETE", f"/channels/{channel_id}")
        return f"Channel '{channel_id}' deleted successfully."

    @tool(
        "discord.channels.list",
        description="List text/voice channels of a server (guild) or list DM channels if guild_id is omitted",
        arguments={
            "guild_id": {"type": "string", "description": "Server (guild) ID. If omitted, returns active DM channels.", "default": ""}
        },
    )
    async def channels_list(self, guild_id: str = "", reason: Optional[str] = None) -> List[Dict[str, Any]]:
        if guild_id:
            channels = await self._api_request("GET", f"/guilds/{guild_id}/channels")
        else:
            channels = await self._api_request("GET", "/users/@me/channels")

        type_map = {0: "text", 1: "dm", 2: "voice", 3: "group_dm", 4: "category", 5: "news", 11: "public_thread", 12: "private_thread"}
        out = []
        for c in channels:
            ctype = type_map.get(c.get("type"), str(c.get("type")))
            recipients = []
            if "recipients" in c:
                for r in c["recipients"]:
                    recipients.append({"id": r.get("id"), "username": r.get("username"), "global_name": r.get("global_name")})
            out.append({
                "id": c.get("id"),
                "name": c.get("name") or (", ".join(r["username"] for r in recipients) if recipients else "DM"),
                "type": ctype,
                "position": c.get("position"),
                "parent_id": c.get("parent_id"),
                "topic": c.get("topic"),
                "recipients": recipients if recipients else None,
            })
        return out

    @tool(
        "discord.messages.history",
        description="Get recent message history from a Discord channel with attachments, embeds, and author info",
        arguments={
            "channel_id": {"type": "string", "description": "Channel or DM ID to read messages from"},
            "limit": {"type": "integer", "description": "Number of messages to fetch (max 100)", "default": 50},
            "before": {"type": "string", "description": "Message ID to fetch messages before (for pagination)", "default": ""}
        },
    )
    async def messages_history(
        self, channel_id: str, limit: int = 50, before: str = "", reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params = {"limit": min(limit, 100)}
        if before:
            params["before"] = before

        messages = await self._api_request("GET", f"/channels/{channel_id}/messages", params=params)
        out = []
        for m in messages:
            author = m.get("author", {})
            attachments = []
            for a in m.get("attachments", []):
                attachments.append({
                    "id": a.get("id"),
                    "filename": a.get("filename"),
                    "url": a.get("url"),
                    "size_bytes": a.get("size"),
                    "content_type": a.get("content_type"),
                })
            embeds = []
            for e in m.get("embeds", []):
                embeds.append({
                    "title": e.get("title"),
                    "description": e.get("description"),
                    "url": e.get("url"),
                })
            out.append({
                "id": m.get("id"),
                "channel_id": m.get("channel_id"),
                "author": {
                    "id": author.get("id"),
                    "username": author.get("username"),
                    "global_name": author.get("global_name"),
                    "bot": author.get("bot", False),
                },
                "content": m.get("content", ""),
                "timestamp": m.get("timestamp"),
                "edited_timestamp": m.get("edited_timestamp"),
                "attachments": attachments,
                "embeds": embeds,
                "reply_to_id": m.get("referenced_message", {}).get("id") if m.get("referenced_message") else None,
            })
        return out

    @tool(
        "discord.messages.send",
        description="Send a message to a Discord channel. Supports text, replies, and file attachments.",
        arguments={
            "channel_id": {"type": "string", "description": "Target channel or DM ID"},
            "content": {"type": "string", "description": "Message text content", "default": ""},
            "reply_to": {"type": "string", "description": "Optional message ID to reply to", "default": ""},
            "file_path": {"type": "string", "description": "Optional local file path to attach and upload", "default": ""}
        },
    )
    async def messages_send(
        self,
        channel_id: str,
        content: str = "",
        reply_to: str = "",
        file_path: str = "",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if content:
            payload["content"] = content
        if reply_to:
            payload["message_reference"] = {"message_id": reply_to}

        if file_path:
            fp = Path(file_path)
            if not fp.exists():
                raise RuntimeError(f"Attachment file not found: {file_path}")
            data = aiohttp.FormData()
            data.add_field("payload_json", json.dumps(payload))
            data.add_field(
                "files[0]",
                fp.read_bytes(),
                filename=fp.name,
                content_type="application/octet-stream",
            )
            res = await self._api_request("POST", f"/channels/{channel_id}/messages", data=data)
        else:
            if not content:
                raise RuntimeError("Either content or file_path must be provided to send a message.")
            res = await self._api_request("POST", f"/channels/{channel_id}/messages", json_data=payload)

        return {
            "id": res.get("id"),
            "channel_id": res.get("channel_id"),
            "content": res.get("content"),
            "timestamp": res.get("timestamp"),
            "info": "Message sent successfully",
        }

    @tool(
        "discord.messages.get",
        description="Get detailed info of a single Discord message by ID",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID"}
        },
    )
    async def messages_get(self, channel_id: str, message_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        m = await self._api_request("GET", f"/channels/{channel_id}/messages/{message_id}")
        author = m.get("author", {})
        return {
            "id": m.get("id"),
            "channel_id": m.get("channel_id"),
            "author": {
                "id": author.get("id"),
                "username": author.get("username"),
                "global_name": author.get("global_name"),
                "bot": author.get("bot", False),
            },
            "content": m.get("content", ""),
            "timestamp": m.get("timestamp"),
            "attachments": m.get("attachments", []),
            "embeds": m.get("embeds", []),
            "reactions": m.get("reactions", []),
        }

    @tool(
        "discord.notifications.list",
        description="List active unread DM channels or recent conversation updates",
    )
    async def notifications_list(self, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        channels = await self._api_request("GET", "/users/@me/channels")
        out = []
        for c in channels:
            last_msg_id = c.get("last_message_id")
            if last_msg_id:
                recipients = c.get("recipients", [])
                out.append({
                    "channel_id": c.get("id"),
                    "name": ", ".join(r.get("username", "") for r in recipients) if recipients else "DM",
                    "last_message_id": last_msg_id,
                    "recipients": [r.get("username") for r in recipients],
                })
        return out[:20]

    @tool(
        "discord.members.list",
        description="Search or list members of a Discord server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Guild (server) ID"},
            "query": {"type": "string", "description": "Optional search term for username/nickname", "default": ""},
            "limit": {"type": "integer", "description": "Max members to return (max 100)", "default": 50}
        },
    )
    async def members_list(
        self, guild_id: str, query: str = "", limit: int = 50, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params = {"limit": min(limit, 100)}
        if query:
            params["query"] = query
            members = await self._api_request("GET", f"/guilds/{guild_id}/members/search", params=params)
        else:
            members = await self._api_request("GET", f"/guilds/{guild_id}/members", params=params)

        out = []
        for m in members:
            u = m.get("user", {})
            out.append({
                "id": u.get("id"),
                "username": u.get("username"),
                "global_name": u.get("global_name"),
                "nickname": m.get("nick"),
                "roles": m.get("roles", []),
                "joined_at": m.get("joined_at"),
            })
        return out

    @tool(
        "discord.members.get",
        description="Get detailed profile of a Discord user by ID",
        arguments={
            "user_id": {"type": "string", "description": "Discord User ID"}
        },
    )
    async def members_get(self, user_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        u = await self._api_request("GET", f"/users/{user_id}")
        return {
            "id": u.get("id"),
            "username": u.get("username"),
            "global_name": u.get("global_name"),
            "discriminator": u.get("discriminator"),
            "avatar": u.get("avatar"),
            "bot": u.get("bot", False),
            "accent_color": u.get("accent_color"),
        }

    @tool(
        "discord.messages.edit",
        description="Edit content of a previously sent Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to edit"},
            "content": {"type": "string", "description": "New message content"}
        },
    )
    async def messages_edit(
        self, channel_id: str, message_id: str, content: str, reason: Optional[str] = None
    ) -> Dict[str, Any]:
        res = await self._api_request("PATCH", f"/channels/{channel_id}/messages/{message_id}", json_data={"content": content})
        return {
            "id": res.get("id"),
            "channel_id": res.get("channel_id"),
            "content": res.get("content"),
            "edited_timestamp": res.get("edited_timestamp"),
            "info": "Message edited successfully",
        }

    @tool(
        "discord.messages.delete",
        description="Delete a Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to delete"}
        },
    )
    async def messages_delete(
        self, channel_id: str, message_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("DELETE", f"/channels/{channel_id}/messages/{message_id}")
        return f"Message '{message_id}' deleted successfully from channel '{channel_id}'."

    @tool(
        "discord.messages.react",
        description="Add an emoji reaction to a Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID"},
            "emoji": {"type": "string", "description": "Emoji to react with (e.g. '👍', '❤️', '🔥', or 'name:id')"}
        },
    )
    async def messages_react(
        self, channel_id: str, message_id: str, emoji: str, reason: Optional[str] = None
    ) -> str:
        import urllib.parse
        encoded_emoji = urllib.parse.quote(emoji)
        await self._api_request("PUT", f"/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me")
        return f"Reacted with '{emoji}' to message '{message_id}' successfully."

    @tool(
        "discord.messages.unreact",
        description="Remove your emoji reaction from a Discord message",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID"},
            "emoji": {"type": "string", "description": "Emoji reaction to remove"}
        },
    )
    async def messages_unreact(
        self, channel_id: str, message_id: str, emoji: str, reason: Optional[str] = None
    ) -> str:
        import urllib.parse
        encoded_emoji = urllib.parse.quote(emoji)
        await self._api_request("DELETE", f"/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me")
        return f"Removed reaction '{emoji}' from message '{message_id}' successfully."

    @tool(
        "discord.messages.pins",
        description="List pinned messages in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"}
        },
    )
    async def messages_pins(
        self, channel_id: str, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        pins = await self._api_request("GET", f"/channels/{channel_id}/pins")
        out = []
        for m in pins:
            author = m.get("author", {})
            out.append({
                "id": m.get("id"),
                "author": author.get("username"),
                "content": m.get("content", ""),
                "timestamp": m.get("timestamp"),
            })
        return out

    @tool(
        "discord.messages.pin",
        description="Pin a message in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to pin"}
        },
    )
    async def messages_pin(
        self, channel_id: str, message_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("PUT", f"/channels/{channel_id}/pins/{message_id}")
        return f"Message '{message_id}' pinned successfully."

    @tool(
        "discord.messages.unpin",
        description="Unpin a message in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"},
            "message_id": {"type": "string", "description": "Message ID to unpin"}
        },
    )
    async def messages_unpin(
        self, channel_id: str, message_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("DELETE", f"/channels/{channel_id}/pins/{message_id}")
        return f"Message '{message_id}' unpinned successfully."

    @tool(
        "discord.typing",
        description="Trigger typing indicator in a Discord channel",
        arguments={
            "channel_id": {"type": "string", "description": "Channel ID"}
        },
    )
    async def typing(
        self, channel_id: str, reason: Optional[str] = None
    ) -> str:
        await self._api_request("POST", f"/channels/{channel_id}/typing")
        return f"Typing indicator triggered in channel '{channel_id}'."

    @tool(
        "discord.messages.search",
        description="Search messages matching a query in a Discord server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Guild (server) ID"},
            "content": {"type": "string", "description": "Search keyword or text pattern"}
        },
    )
    async def messages_search(
        self, guild_id: str, content: str, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        res = await self._api_request("GET", f"/guilds/{guild_id}/messages/search", params={"content": content})
        messages = res.get("messages", [])
        out = []
        for group in messages:
            for m in group:
                author = m.get("author", {})
                out.append({
                    "id": m.get("id"),
                    "channel_id": m.get("channel_id"),
                    "author": author.get("username"),
                    "content": m.get("content", ""),
                    "timestamp": m.get("timestamp"),
                })
        return out

    @tool(
        "discord.profile.update",
        description="Update account profile details including Display Name (global_name), Bio/About Me, Avatar image, Banner image, or accent color",
        arguments={
            "global_name": {"type": "string", "description": "New Display Name", "default": ""},
            "bio": {"type": "string", "description": "New About Me / Bio text", "default": ""},
            "avatar_path": {"type": "string", "description": "Local image file path to upload as avatar", "default": ""},
            "banner_path": {"type": "string", "description": "Local image file path to upload as banner", "default": ""},
            "accent_color": {"type": "integer", "description": "Integer color code (e.g. 16711680 for red)", "default": 0}
        },
    )
    async def profile_update(
        self,
        global_name: str = "",
        bio: str = "",
        avatar_path: str = "",
        banner_path: str = "",
        accent_color: int = 0,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if global_name:
            payload["global_name"] = global_name
        if bio:
            payload["bio"] = bio
        if avatar_path:
            payload["avatar"] = _file_to_base64_data_uri(avatar_path)
        if banner_path:
            payload["banner"] = _file_to_base64_data_uri(banner_path)
        if accent_color > 0:
            payload["accent_color"] = accent_color

        if not payload:
            raise RuntimeError("At least one profile parameter must be specified to update profile.")

        res = await self._api_request("PATCH", "/users/@me", json_data=payload)
        return {
            "id": res.get("id"),
            "username": res.get("username"),
            "global_name": res.get("global_name"),
            "bio": res.get("bio"),
            "avatar": res.get("avatar"),
            "banner": res.get("banner"),
            "info": "Profile updated successfully.",
        }

    @tool(
        "discord.members.set_nickname",
        description="Change your nickname in a specific Discord server (guild)",
        arguments={
            "guild_id": {"type": "string", "description": "Guild (server) ID"},
            "nickname": {"type": "string", "description": "New nickname for the server (empty string to reset)"}
        },
    )
    async def members_set_nickname(
        self, guild_id: str, nickname: str = "", reason: Optional[str] = None
    ) -> str:
        await self._api_request("PATCH", f"/guilds/{guild_id}/members/@me", json_data={"nick": nickname})
        new_name = nickname if nickname else "default"
        return f"Server nickname in guild '{guild_id}' changed to '{new_name}' successfully."

    @tool(
        "discord.dm.list",
        description="List all active Direct Message (DM) and Group DM conversations with recipient profiles and last message IDs",
    )
    async def dm_list(self, reason: Optional[str] = None) -> List[Dict[str, Any]]:
        channels = await self._api_request("GET", "/users/@me/channels")
        out = []
        for c in channels:
            ctype = "group_dm" if c.get("type") == 3 else "dm"
            recipients = [
                {
                    "id": r.get("id"),
                    "username": r.get("username"),
                    "global_name": r.get("global_name"),
                    "avatar": r.get("avatar"),
                }
                for r in c.get("recipients", [])
            ]
            out.append({
                "channel_id": c.get("id"),
                "type": ctype,
                "name": c.get("name") or (", ".join(r["username"] for r in recipients) if recipients else "DM"),
                "last_message_id": c.get("last_message_id"),
                "recipients": recipients,
            })
        return out

    @tool(
        "discord.dm.open",
        description="Open or get a Direct Message (DM) channel with a target user by User ID",
        arguments={
            "user_id": {"type": "string", "description": "Target Discord User ID"}
        },
    )
    async def dm_open(self, user_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        dm = await self._api_request("POST", "/users/@me/channels", json_data={"recipient_id": user_id})
        recipients = [
            {"id": r.get("id"), "username": r.get("username"), "global_name": r.get("global_name")}
            for r in dm.get("recipients", [])
        ]
        return {
            "channel_id": dm.get("id"),
            "recipient": recipients[0] if recipients else None,
            "info": f"DM channel opened with user '{user_id}'. Use channel_id to read/send messages.",
        }

    @tool(
        "discord.dm.send",
        description="Send a Direct Message (DM) directly to a target User ID or DM Channel ID",
        arguments={
            "recipient_id": {"type": "string", "description": "Target Discord User ID or DM Channel ID"},
            "content": {"type": "string", "description": "Message text content", "default": ""},
            "reply_to": {"type": "string", "description": "Optional message ID to reply to", "default": ""},
            "file_path": {"type": "string", "description": "Optional local file path to attach and upload", "default": ""}
        },
    )
    async def dm_send(
        self,
        recipient_id: str,
        content: str = "",
        reply_to: str = "",
        file_path: str = "",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        target_channel_id = recipient_id
        try:
            dm = await self._api_request("POST", "/users/@me/channels", json_data={"recipient_id": recipient_id})
            if dm and "id" in dm:
                target_channel_id = dm["id"]
        except Exception:
            target_channel_id = recipient_id

        return await self.messages_send(channel_id=target_channel_id, content=content, reply_to=reply_to, file_path=file_path)

    @tool(
        "discord.gateway.connect",
        description="Start real-time WebSocket connection to Discord Gateway v10 to receive live notifications (DMs, mentions, voice events, reactions)",
    )
    async def gateway_connect(self, reason: Optional[str] = None) -> str:
        if not self._token:
            raise RuntimeError("Not logged in. Call discord.login(token) first.")
        if self._gateway_task and not self._gateway_task.done():
            return "Discord Gateway WebSocket is already connected and listening for live events."

        self._gateway_task = asyncio.create_task(self._gateway_listener())
        await asyncio.sleep(1.0)
        return "Connected to Discord Gateway WebSocket successfully. Real-time events and notifications are active."

    @tool(
        "discord.gateway.disconnect",
        description="Disconnect from Discord Gateway WebSocket listener",
    )
    async def gateway_disconnect(self, reason: Optional[str] = None) -> str:
        if self._gateway_task:
            self._gateway_task.cancel()
            self._gateway_task = None
        self._gateway_connected = False
        self._gateway_ws = None
        return "Disconnected from Discord Gateway WebSocket."

    @tool(
        "discord.gateway.status",
        description="Get current status of Discord Gateway live connection and notification cache size",
    )
    async def gateway_status(self, reason: Optional[str] = None) -> Dict[str, Any]:
        unread_count = sum(1 for n in self._notifications_cache if not n.get("read"))
        return {
            "connected": self._gateway_connected,
            "listening": bool(self._gateway_task and not self._gateway_task.done()),
            "cached_notifications_total": len(self._notifications_cache),
            "unread_notifications_count": unread_count,
            "info": "Gateway status active" if self._gateway_connected else "Gateway disconnected. Use discord.gateway.connect to start live stream.",
        }

    @tool(
        "discord.notifications.feed",
        description="Read cached real-time notifications (DMs, mentions, reactions, voice_state events). Automatically marks returned items as read.",
        arguments={
            "unread_only": {"type": "boolean", "description": "Filter to return only unread notifications", "default": True},
            "type_filter": {"type": "string", "description": "Optional type filter: 'dm', 'mention', 'message', 'voice_state', 'reaction'", "default": ""},
            "limit": {"type": "integer", "description": "Max notifications to return (default 20)", "default": 20}
        },
    )
    async def notifications_feed(
        self, unread_only: bool = True, type_filter: str = "", limit: int = 20, reason: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        result = []
        for n in self._notifications_cache:
            if unread_only and n.get("read"):
                continue
            if type_filter and n.get("type") != type_filter.lower():
                continue
            result.append(n)
            n["read"] = True
            if len(result) >= limit:
                break

        self._save_notifications_cache()
        return result

    @tool(
        "discord.notifications.clear",
        description="Clear cached notifications list or delete a specific notification by ID",
        arguments={
            "notification_id": {"type": "string", "description": "Optional specific notification ID to remove. If empty, clears all notifications.", "default": ""}
        },
    )
    async def notifications_clear(self, notification_id: str = "", reason: Optional[str] = None) -> str:
        if notification_id:
            self._notifications_cache = [n for n in self._notifications_cache if n.get("id") != notification_id]
            msg = f"Notification '{notification_id}' cleared."
        else:
            self._notifications_cache.clear()
            msg = "All cached notifications cleared."

        self._save_notifications_cache()
        return msg

    @tool(
        "discord.presence.update",
        description="Update your Discord online status (online, dnd, idle, invisible) and custom activity/status with optional emoji and text",
        arguments={
            "status": {"type": "string", "description": "Online status: 'online', 'dnd' (Do Not Disturb), 'idle', 'invisible'", "default": "online"},
            "activity_type": {"type": "string", "description": "Activity type: 'custom', 'playing', 'listening', 'watching', 'streaming', 'competing'", "default": "custom"},
            "activity_name": {"type": "string", "description": "Custom status text or game/stream name", "default": ""},
            "emoji": {"type": "string", "description": "Optional emoji for custom status (e.g. '🤖', '🔥', '⚡')", "default": ""}
        },
    )
    async def presence_update(
        self,
        status: str = "online",
        activity_type: str = "custom",
        activity_name: str = "",
        emoji: str = "",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        valid_statuses = ["online", "dnd", "idle", "invisible"]
        status_clean = status.lower().strip()
        if status_clean not in valid_statuses:
            raise RuntimeError(f"Invalid status '{status}'. Must be one of: {valid_statuses}")

        self._current_presence = {
            "status": status_clean,
            "activity_type": activity_type.lower().strip(),
            "activity_name": activity_name,
            "emoji": emoji,
        }

        if self._gateway_ws and not self._gateway_ws.closed:
            payload = {"op": 3, "d": self._build_presence_payload()}
            await self._gateway_ws.send_json(payload)
            info_str = f"Presence updated to '{status_clean}' and broadcasted live over Discord Gateway WebSocket."
        else:
            info_str = f"Presence saved as '{status_clean}'. Will be broadcasted live as soon as discord.gateway.connect is active."

        return {
            "status": status_clean,
            "activity_type": activity_type,
            "activity_name": activity_name,
            "emoji": emoji,
            "gateway_connected": self._gateway_connected,
            "info": info_str,
        }
