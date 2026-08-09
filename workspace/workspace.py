"""UnAI Discord Workspace implementation.

Subclasses Workspace from UnAI SDK. Interacts natively with Discord REST API v10
using user or bot tokens. Provides full capability for reading servers, channels,
messages with attachments, sending messages, replies, member lookup, and notifications.

Follows ADR-0004 for one-shot login tool state management.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import aiohttp

from unai.sdk import Workspace, tool

DISCORD_API_BASE = "https://discord.com/api/v10"


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
        self._load_saved_token()

    def _load_saved_token(self) -> None:
        tf = _get_token_file()
        if tf.exists():
            try:
                data = json.loads(tf.read_text())
                self._token = data.get("token")
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
