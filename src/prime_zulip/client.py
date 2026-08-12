"""Low-level async Zulip API client for the bridge."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

MESSAGE_EVENT_TYPES = ["message", "reaction"]


class ZulipAPIError(Exception):
    """Raised when the Zulip API returns an error."""


class BadEventQueueError(ZulipAPIError):
    """Raised when Zulip reports the event queue is gone (BAD_EVENT_QUEUE_ID).

    Queue IDs expire after roughly ten minutes. The poll loop catches this
    specifically so it can re-register rather than spinning on 400s.
    """

    def __init__(self, msg: str, payload: dict[str, Any] | None = None) -> None:
        super().__init__(msg)
        self.payload = payload or {}


class ZulipClient:
    """Async HTTP client for the Zulip REST API using basic auth."""

    def __init__(
        self,
        site_url: str,
        email: str,
        api_key: str,
        *,
        timeout: float = 30.0,
    ):
        self.site_url = site_url.rstrip("/")
        self.email = email
        self.api_base = f"{self.site_url}/api/v1"
        self._client = httpx.AsyncClient(
            auth=(email, api_key),
            timeout=timeout,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> ZulipClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ── identity ──────────────────────────────────────────────────

    async def get_profile(self) -> dict[str, Any]:
        """Return the authenticated user's profile."""
        return await self._get("/users/me")

    # ── event queue ───────────────────────────────────────────────

    async def register_queue(
        self, *, event_types: list[str] | None = None
    ) -> dict[str, Any]:
        """Register a new event queue."""
        types = event_types or MESSAGE_EVENT_TYPES
        payload = await self._post(
            "/register",
            data={
                "event_types": json.dumps(types),
                "apply_markdown": "false",
            },
        )
        if payload.get("result") == "error":
            msg = payload.get("msg") or payload.get("code") or "register failed"
            raise ZulipAPIError(msg)
        return payload

    async def get_events(
        self,
        queue_id: str,
        last_event_id: int,
    ) -> dict[str, Any]:
        """Long-poll for events from the queue."""
        return await self._get(
            "/events",
            params={
                "queue_id": queue_id,
                "last_event_id": last_event_id,
            },
            timeout=120.0,
        )

    async def delete_queue(self, queue_id: str) -> None:
        """Delete an event queue."""
        try:
            await self._delete("/events", params={"queue_id": queue_id})
        except Exception:
            pass

    # ── messages ──────────────────────────────────────────────────

    async def send_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Send a Zulip message. Returns the API response."""
        resp = await self._post("/messages", data=payload)
        if resp.get("result") != "success":
            msg = resp.get("msg") or resp.get("code") or "send failed"
            raise ZulipAPIError(msg)
        return resp

    async def edit_message(self, message_id: int, content: str) -> dict[str, Any]:
        """Edit an existing message."""
        return await self._patch(f"/messages/{message_id}", data={"content": content})

    async def delete_message(self, message_id: int) -> None:
        """Delete a message."""
        await self._delete(f"/messages/{message_id}")

    async def get_message(self, message_id: int) -> dict[str, Any]:
        """Get a single message by ID."""
        return await self._get(f"/messages/{message_id}")

    async def get_messages(
        self,
        anchor: str = "newest",
        num_before: int = 100,
        num_after: int = 0,
        narrow: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Get messages from the server with optional narrowing."""
        params: dict[str, Any] = {
            "anchor": anchor,
            "num_before": num_before,
            "num_after": num_after,
        }
        if narrow:
            params["narrow"] = json.dumps(narrow)
        return await self._get("/messages", params=params)

    async def update_message_flags(
        self,
        *,
        add: str | None = None,
        remove: str | None = None,
        messages: list[int] | None = None,
    ) -> dict[str, Any]:
        """Add or remove flags from messages."""
        data: dict[str, Any] = {}
        if add:
            data["op"] = "add"
            data["flag"] = add
        elif remove:
            data["op"] = "remove"
            data["flag"] = remove
        if messages:
            data["messages"] = json.dumps(messages)
        return await self._post("/messages/flags", data=data)

    # ── typing ────────────────────────────────────────────────────

    async def send_typing(
        self,
        *,
        op: str = "start",
        type: str = "direct",
        to: list[int] | None = None,
        topic: str | None = None,
    ) -> dict[str, Any]:
        """Send a typing notification."""
        data: dict[str, Any] = {"op": op, "type": type}
        if to:
            data["to"] = json.dumps(to)
        if topic:
            data["topic"] = topic
        return await self._post("/typing", data=data)

    # ── reactions ─────────────────────────────────────────────────

    async def add_reaction(
        self,
        message_id: int,
        emoji_name: str,
        emoji_code: str | None = None,
    ) -> dict[str, Any]:
        """Add an emoji reaction to a message."""
        data: dict[str, Any] = {"emoji_name": emoji_name}
        if emoji_code:
            data["emoji_code"] = emoji_code
        return await self._post(f"/messages/{message_id}/reactions", data=data)

    async def remove_reaction(
        self,
        message_id: int,
        emoji_name: str,
        emoji_code: str | None = None,
    ) -> dict[str, Any]:
        """Remove an emoji reaction from a message."""
        data: dict[str, Any] = {"emoji_name": emoji_name}
        if emoji_code:
            data["emoji_code"] = emoji_code
        return await self._delete(f"/messages/{message_id}/reactions", params=data)

    # ── uploads ───────────────────────────────────────────────────

    async def upload_file(
        self,
        file_path: str,
        *,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """Upload a file to Zulip. Returns {uri, url}."""
        import mimetypes
        from pathlib import Path

        path = Path(file_path)
        if filename is None:
            filename = path.name
        if content_type is None:
            content_type, _ = mimetypes.guess_type(str(path))
            content_type = content_type or "application/octet-stream"

        with open(path, "rb") as f:
            files = {"file": (filename, f, content_type)}
            # Use a separate request for multipart upload
            resp = await self._client.post(
                f"{self.api_base}/user_uploads",
                files=files,
            )
        self._raise_for_status(resp)
        return resp.json()

    # ── streams ───────────────────────────────────────────────────

    async def get_streams(self) -> list[dict[str, Any]]:
        """Get all streams the user can see."""
        resp = await self._get("/streams")
        return resp.get("streams", [])

    async def get_stream_id(self, stream_name: str) -> int | None:
        """Look up a stream ID by name."""
        try:
            resp = await self._get("/get_stream_id", params={"stream": stream_name})
            return resp.get("stream_id")
        except ZulipAPIError:
            return None

    async def get_topics(self, stream_id: int) -> list[dict[str, Any]]:
        """Get topics in a stream."""
        resp = await self._get(f"/users/me/{stream_id}/topics")
        return resp.get("topics", [])

    # ── users ─────────────────────────────────────────────────────

    async def get_users(self) -> list[dict[str, Any]]:
        """Get all users in the organization."""
        resp = await self._get("/users")
        return resp.get("members", [])

    async def get_user(self, user_id: int) -> dict[str, Any]:
        """Get a single user by ID."""
        return await self._get(f"/users/{user_id}")

    # ── internal helpers ──────────────────────────────────────────

    async def _get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        resp = await self._client.get(f"{self.api_base}{path}", **kwargs)
        self._raise_for_status(resp)
        return resp.json()

    async def _post(self, path: str, **kwargs: Any) -> dict[str, Any]:
        resp = await self._client.post(f"{self.api_base}{path}", **kwargs)
        self._raise_for_status(resp)
        return resp.json()

    async def _patch(self, path: str, **kwargs: Any) -> dict[str, Any]:
        resp = await self._client.patch(f"{self.api_base}{path}", **kwargs)
        self._raise_for_status(resp)
        return resp.json()

    async def _delete(self, path: str, **kwargs: Any) -> dict[str, Any]:
        resp = await self._client.delete(f"{self.api_base}{path}", **kwargs)
        self._raise_for_status(resp)
        return resp.json()

    @staticmethod
    def _raise_for_status(resp: httpx.Response) -> None:
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            # Parse the JSON body so callers can branch on Zulip's machine-
            # readable ``code`` field rather than scraping the message text.
            # The most important case is BAD_EVENT_QUEUE_ID: Zulip returns it
            # as HTTP 400 when the event queue has expired, and without
            # surfacing it here the poll loop never gets to re-register.
            payload: dict[str, Any] = {}
            try:
                payload = resp.json()
            except Exception:
                payload = {}

            code = str(payload.get("code", "")).upper()
            if code == "BAD_EVENT_QUEUE_ID":
                raise BadEventQueueError(
                    payload.get("msg") or "BAD_EVENT_QUEUE_ID",
                    payload,
                ) from None

            body = resp.text[:500] if resp.text else ""
            raise ZulipAPIError(f"HTTP {resp.status_code}: {body}") from None
