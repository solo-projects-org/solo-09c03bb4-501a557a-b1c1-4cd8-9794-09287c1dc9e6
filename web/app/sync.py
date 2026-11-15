"""Delivery of published days to the analysis group's local receiver.

Design:
* The outbox row is written in the same transaction as ``published`` (see
  ``db.publish_version``), so an accepted day is always queued for delivery.
* Either web instance may send; a short DB lease (``db.claim_delivery``)
  makes sure only one of them transmits a given revision at a time.
* A send failure only updates the outbox row -- the published day remains
  intact and readable locally.
* Only the newest locally published revision of a day is eligible. A late
  older revision is ``superseded`` and is never presented as synced.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone

from . import db
from .receiver_client import (
    ReceiverClient,
    ReceiverRejected,
    ReceiverUnavailable,
)

log = logging.getLogger("sync")

# Retry pacing for the background sender. Manual retries bypass the wait.
POLL_SECONDS = 2.0
RETRY_BASE_SECONDS = float(os.environ.get("SYNC_RETRY_BASE_SECONDS", "5"))
RETRY_MAX_SECONDS = float(os.environ.get("SYNC_RETRY_MAX_SECONDS", "120"))


class DeliveryError(Exception):
    """Could not deliver (transient or rejected); published data is untouched."""


class DeliveryBusyError(DeliveryError):
    """Another web instance currently holds the send lease for this revision."""


def build_payload(db_path: str, section: str, date: str, revision: int) -> dict:
    """Assemble the complete day exactly as the receiver will verify it."""
    published = db.get_published(db_path, section, date)
    if published is None:
        raise DeliveryError(f"no published day for {section} {date}")
    if int(published["revision"]) != revision:
        raise DeliveryError(
            f"published revision is {published['revision']}, not {revision}"
        )
    rows = published["rows"]
    return {
        "section": section,
        "date": date,
        "revision": int(revision),
        "complete": True,
        "row_count": len(rows),
        "digest": db.delivery_digest(section, date, revision, rows),
        "published_at": published["published_at"],
        "published_by": published["published_by"],
        "rows": [
            {
                "time": r["time"],
                "nh3n": r["nh3n"],
                "source_page": r["source_page"],
            }
            for r in rows
        ],
    }


def deliver_once(
    db_path: str,
    section: str,
    date: str,
    revision: int,
    worker: str,
    client: ReceiverClient,
) -> dict:
    """Attempt exactly one delivery now.

    Returns ``{"status": "sent", ...}`` on success. Raises ``DeliveryError`` on
    failure; in both cases the locally published day is left untouched.
    """
    attempts = db.claim_delivery(db_path, section, date, revision, worker)
    if attempts is None:
        row = db.get_outbox(db_path, section, date, revision)
        if row is None:
            raise DeliveryError(f"no delivery queued for revision {revision}")
        if row["status"] == "sent":
            return {"status": "sent", "revision": revision, "duplicate": True}
        if row["status"] == "sending":
            raise DeliveryBusyError(
                f"delivery is being sent by {row['claimed_by']}; try again shortly"
            )
        raise DeliveryError(
            f"delivery is {row['status']} and cannot be sent by {worker} right now"
        )

    try:
        payload = build_payload(db_path, section, date, revision)
    except Exception as exc:
        db.mark_delivery_failed(
            db_path, section, date, revision, worker, attempts, str(exc)
        )
        raise DeliveryError(str(exc)) from exc

    try:
        ack = client.deliver(payload)
    except ReceiverRejected as exc:
        # Permanent: mark rejected so the UI can show it, but keep the local day.
        db.mark_delivery_failed(
            db_path, section, date, revision, worker, attempts, str(exc),
            rejected=True,
        )
        raise DeliveryError(str(exc)) from exc
    except ReceiverUnavailable as exc:
        db.mark_delivery_failed(
            db_path, section, date, revision, worker, attempts, str(exc)
        )
        raise DeliveryError(str(exc)) from exc
    except Exception as exc:  # defensive: never leave a 'sending' row stuck
        db.mark_delivery_failed(
            db_path, section, date, revision, worker, attempts,
            f"unexpected delivery error: {exc}",
        )
        raise DeliveryError(str(exc)) from exc

    db.mark_delivery_sent(
        db_path, section, date, revision, worker, attempts, remote=ack
    )
    log.info(
        "delivered %s %s revision %s via %s: %s",
        section, date, revision, worker, ack,
    )
    return {"status": "sent", "revision": revision, "acknowledgment": ack}


def _retry_delay(attempts: int) -> float:
    return min(RETRY_BASE_SECONDS * (2 ** max(0, attempts - 1)), RETRY_MAX_SECONDS)


def _next_attempt_due(row: dict) -> bool:
    if row["status"] == "pending":
        return True
    last = row.get("last_attempt_at")
    if not last:
        return True
    try:
        at = datetime.fromisoformat(last)
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - at).total_seconds()
    except ValueError:
        return True
    return age >= _retry_delay(int(row.get("attempts") or 0))


class BackgroundSender:
    """Runs in both web instances; the DB lease arbitrates between them."""

    def __init__(self, db_path: str, receiver_url: str, worker: str) -> None:
        self._db_path = db_path
        self._receiver_url = receiver_url
        self._worker = worker
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._kick = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name=f"sync-{self._worker}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._kick.set()

    def kick(self) -> None:
        """Wake the loop, e.g. right after a publish or a manual retry."""
        self._kick.set()

    def _run(self) -> None:
        client = ReceiverClient(self._receiver_url)
        log.info("background sender for %s target=%s", self._worker, self._receiver_url)
        while not self._stop.is_set():
            try:
                db.prune_delivery_claims(self._db_path)
                delivered_any = self._pass(client)
            except Exception:
                log.exception("sender pass failed")
                delivered_any = False
            if delivered_any:
                continue  # drain the queue without sleeping
            self._kick.wait(POLL_SECONDS)
            self._kick.clear()

    def _pass(self, client: ReceiverClient) -> bool:
        delivered = False
        for row in db.pending_deliveries(self._db_path):
            if not _next_attempt_due(row):
                continue
            try:
                deliver_once(
                    self._db_path,
                    row["section"],
                    row["date"],
                    int(row["revision"]),
                    self._worker,
                    client,
                )
                delivered = True
            except DeliveryError as exc:
                log.warning(
                    "delivery %s %s rev %s failed: %s",
                    row["section"], row["date"], row["revision"], exc,
                )
        return delivered


def sync_state(db_path: str, section: str, date: str, receiver_url: str) -> dict:
    """The UI-facing view: is the *current* published revision synced?

    Only a ``sent`` outbox row whose revision equals the locally published
    revision counts as synced. Superseded/rejected/failed older rows are
    reported as history and never as the current sync state.
    """
    published = db.get_published(db_path, section, date)
    outbox = db.list_outbox(db_path, limit=100, section=section, date=date)
    outbox.sort(key=lambda r: int(r["revision"]))
    current_revision = None if published is None else int(published["revision"])
    current_row = next(
        (r for r in outbox if int(r["revision"]) == current_revision), None
    )

    if published is None:
        state = "nothing_published"
    elif current_row is None:
        state = "not_queued"
    elif current_row["status"] == "sent":
        state = "synced"
    elif current_row["status"] in ("pending", "sending"):
        state = "pending"
    elif current_row["status"] == "rejected":
        state = "rejected"
    else:
        state = "failed"

    remote = None
    remote_error = None
    try:
        # Status is rendered on every UI poll: use a short timeout so a down
        # receiver degrades to "unknown" quickly instead of stalling the page.
        remote = ReceiverClient(receiver_url, timeout=1.5).day(section, date)
    except ReceiverUnavailable as exc:
        remote_error = str(exc)
    except ReceiverRejected as exc:
        remote_error = str(exc)

    receiver_revision = None if remote is None else int(remote["revision"])
    return {
        "section": section,
        "date": date,
        "published_revision": current_revision,
        "published_rows": None if published is None else len(published["rows"]),
        "published_at": None if published is None else published["published_at"],
        "sync_state": state,
        "current_delivery": current_row,
        "deliveries": outbox,
        "receiver_revision": receiver_revision,
        "receiver_reachable": remote_error is None,
        "receiver_error": remote_error,
        "receiver_matches": (
            current_revision is not None and receiver_revision == current_revision
        ),
    }
