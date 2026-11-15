"""HTTP client for the local receiving end (analysis group's archive).

The sender distinguishes retryable transport/server problems from permanent
semantic rejections:

* ``ReceiverUnavailable`` -- network error, timeout, 5xx: retry later;
* ``ReceiverRejected``    -- the receiver refused the version (stale revision,
                            conflicting content, incomplete day): retrying the
                            same bytes cannot succeed, surface it for an
                            operator and never mark the day as synced.
"""
from __future__ import annotations

from typing import Optional

import httpx


class ReceiverError(Exception):
    """Base class for receiving-end problems."""


class ReceiverUnavailable(ReceiverError):
    """Transient: the delivery may be retried as-is later."""


class ReceiverRejected(ReceiverError):
    """Permanent: this exact day/revision will never be accepted."""

    def __init__(self, message: str, status_code: int, detail: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail or {}


class ReceiverClient:
    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self._base, timeout=self._timeout)

    def deliver(self, payload: dict) -> dict:
        """Send one complete day. Returns the receiver's acknowledgment."""
        try:
            with self._client() as c:
                resp = c.post("/api/deliveries", json=payload)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError,
                httpx.HTTPError) as exc:
            raise ReceiverUnavailable(f"receiver unreachable: {exc}") from exc

        if resp.status_code >= 500:
            raise ReceiverUnavailable(f"receiver returned {resp.status_code}")
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail")
            except ValueError:
                detail = None
            if isinstance(detail, dict):
                message = detail.get("error", "rejected")
                extra = detail.get("message")
                text = f"receiver rejected ({message})"
                if extra:
                    text += f": {extra}"
            else:
                text = f"receiver rejected: {resp.status_code} {resp.text[:200]}"
            raise ReceiverRejected(text, resp.status_code,
                                   detail if isinstance(detail, dict) else None)
        try:
            return resp.json()
        except ValueError as exc:
            raise ReceiverUnavailable(f"invalid receiver response: {exc}") from exc

    def day(self, section: str, date: str) -> Optional[dict]:
        """Current full day at the receiver, or None if absent/unreachable check.

        Raises ``ReceiverUnavailable`` on transport errors so callers can show
        "unknown" rather than pretending the day is synced.
        """
        try:
            with self._client() as c:
                resp = c.get(f"/api/days/{section}/{date}")
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError,
                httpx.HTTPError) as exc:
            raise ReceiverUnavailable(f"receiver unreachable: {exc}") from exc
        if resp.status_code == 404:
            return None
        if resp.status_code >= 500:
            raise ReceiverUnavailable(f"receiver returned {resp.status_code}")
        if resp.status_code >= 400:
            raise ReceiverRejected(
                f"receiver rejected status query: {resp.status_code}", resp.status_code
            )
        try:
            return resp.json()
        except ValueError as exc:
            raise ReceiverUnavailable(f"invalid receiver response: {exc}") from exc

    def health(self) -> bool:
        try:
            with self._client() as c:
                resp = c.get("/health")
                return resp.status_code == 200
        except httpx.HTTPError:
            return False
