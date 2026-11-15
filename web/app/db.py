"""SQLite persistence for the collection web service.

The database is shared by the two replicated web processes through a mounted
volume, so SQLite (WAL mode) plus ``BEGIN IMMEDIATE`` is the cross-process
concurrency boundary:

* only one RUNNING collection per (section, date) may exist;
* a late collection may never overwrite a published day whose source revision
  is newer (``StaleRevisionError``);
* publication replaces the whole day atomically -- partial pages never become
  visible.

Publication and delivery queueing happen in the *same* SQLite transaction:
whenever a complete day is published an outbox row for that revision is
registered, so a crash right after the commit can never lose the delivery.
Delivery itself only ever updates ``deliveries`` -- a failed send never rolls
back ``published``.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS published (
    section      TEXT NOT NULL,
    date         TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    time         TEXT NOT NULL,
    nh3n         REAL NOT NULL,
    source_page  INTEGER NOT NULL,
    published_at TEXT NOT NULL,
    published_by TEXT NOT NULL,
    PRIMARY KEY (section, date, time)
);

CREATE TABLE IF NOT EXISTS runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    section        TEXT NOT NULL,
    date           TEXT NOT NULL,
    worker         TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('running','success','failed','interrupted')),
    revision       INTEGER,
    rows_collected INTEGER NOT NULL DEFAULT 0,
    page_attempts  INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    started_at     TEXT NOT NULL,
    finished_at    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_runs_active
    ON runs (section, date) WHERE status = 'running';

-- Transactional outbox: one row per published (section, date, revision).
-- Rows are registered in the same transaction that publishes the day, so an
-- accepted day can never be lost from the delivery queue.
CREATE TABLE IF NOT EXISTS deliveries (
    section         TEXT NOT NULL,
    date            TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (
                        status IN ('pending','sending','sent','failed',
                                   'superseded','rejected')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    last_attempt_at TEXT,
    sent_at         TEXT,
    sent_by         TEXT,
    remote_result   TEXT,
    claimed_at      TEXT,
    claimed_by      TEXT,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (section, date, revision)
);

CREATE INDEX IF NOT EXISTS ix_deliveries_status
    ON deliveries (status, created_at);
"""


class ActiveRunError(Exception):
    """Another collection for this section/day is still in progress."""


class StaleRevisionError(Exception):
    """A newer revision of the day is already published locally."""


class DeliveryNotFoundError(Exception):
    """No delivery for the requested published revision."""


# A worker that claims a delivery keeps the lease for this long. Another web
# process may take over afterwards (e.g. the first one was killed mid-send).
DELIVERY_LEASE_SECONDS = 60

# Retry backoff for failed deliveries picked up by the background sender.
# Manual retries ignore it.
DELIVERY_RETRY_BASE_SECONDS = 5
DELIVERY_RETRY_MAX_SECONDS = 120


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def delivery_digest(section: str, date: str, revision: int, rows: list[dict]) -> str:
    """Stable content identity for one published day.

    Both the sender and the local receiver compute this independently; the
    receiver uses it to treat an identical re-send as a duplicate instead of a
    conflicting overwrite.
    """
    canonical = {
        "section": section,
        "date": date,
        "revision": int(revision),
        "rows": sorted(
            (
                {
                    "time": str(r["time"]),
                    "nh3n": format(float(r["nh3n"]), ".6f"),
                    "source_page": int(r["source_page"]),
                }
                for r in rows
            ),
            key=lambda r: r["time"],
        ),
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def recover_interrupted(db_path: str, worker: Optional[str] = None) -> int:
    """Mark runs left 'running' by a crashed/restarted worker.

    With two web instances sharing the database, recovery is scoped to the
    restarting worker: otherwise starting web2 would wrongly interrupt a
    collection still running in web1. Pass ``worker=None`` to recover all
    (used by single-process deployments/tests).
    """
    conn = get_conn(db_path)
    try:
        now = utc_now()
        if worker is None:
            cur = conn.execute(
                "UPDATE runs SET status='interrupted', "
                "error='worker restarted before collection finished', finished_at=? "
                "WHERE status='running'",
                (now,),
            )
        else:
            cur = conn.execute(
                "UPDATE runs SET status='interrupted', "
                "error='worker restarted before collection finished', finished_at=? "
                "WHERE status='running' AND worker=?",
                (now, worker),
            )
        return cur.rowcount
    finally:
        conn.close()


def start_run(db_path: str, section: str, date: str, worker: str) -> int:
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "INSERT INTO runs (section, date, worker, status, started_at) "
                "VALUES (?, ?, ?, 'running', ?)",
                (section, date, worker, utc_now()),
            )
            conn.execute("COMMIT")
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            raise ActiveRunError(
                f"a collection for {section} {date} is already running"
            )
    finally:
        conn.close()


def update_progress(
    db_path: str,
    run_id: int,
    revision: Optional[int],
    rows_collected: int,
    page_attempts: int,
) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE runs SET revision=?, rows_collected=?, page_attempts=? WHERE id=?",
            (revision, rows_collected, page_attempts, run_id),
        )
    finally:
        conn.close()


def fail_run(db_path: str, run_id: int, error: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(
            "UPDATE runs SET status='failed', error=?, finished_at=? WHERE id=?",
            (error[:500], utc_now(), run_id),
        )
    finally:
        conn.close()


def publish_version(
    db_path: str,
    section: str,
    date: str,
    revision: int,
    rows: list[dict],
    worker: str,
    run_id: int,
) -> dict:
    """Atomically replace the whole day's published version.

    Raises ``StaleRevisionError`` if a strictly newer source revision is
    already published. Re-publishing the same revision is idempotent.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT revision FROM published WHERE section=? AND date=? LIMIT 1",
            (section, date),
        ).fetchone()
        current = None if row is None else int(row["revision"])
        if current is not None and current > revision:
            conn.execute("ROLLBACK")
            raise StaleRevisionError(
                f"refusing to publish revision {revision}: revision {current} "
                f"is already published for {section} {date}"
            )

        now = utc_now()
        conn.execute(
            "DELETE FROM published WHERE section=? AND date=?", (section, date)
        )
        conn.executemany(
            "INSERT INTO published (section, date, revision, time, nh3n, "
            "source_page, published_at, published_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    section,
                    date,
                    revision,
                    r["time"],
                    float(r["nh3n"]),
                    int(r["source_page"]),
                    now,
                    worker,
                )
                for r in rows
            ],
        )
        conn.execute(
            "UPDATE runs SET status='success', revision=?, rows_collected=?, "
            "finished_at=?, error=NULL WHERE id=?",
            (revision, len(rows), now, run_id),
        )
        # Transactional outbox: the delivery intent is part of the same
        # commit as the published day. A crash after this point cannot lose it.
        _enqueue_delivery_tx(conn, section, date, revision, now)
        conn.execute("COMMIT")
        return {
            "action": "created" if current is None else
                      ("replaced" if current < revision else "unchanged_same_revision"),
            "previous_revision": current,
            "published_revision": revision,
            "rows": len(rows),
        }
    finally:
        conn.close()


def _enqueue_delivery_tx(
    conn: sqlite3.Connection, section: str, date: str, revision: int, now: str
) -> None:
    """Register (or keep) the outbox row for a published revision.

    Older undelivered revisions become ``superseded`` -- only the newest
    locally published revision is ever transmitted. A revision already marked
    ``sent`` is left alone (re-publishing the same revision is idempotent).
    """
    existing = conn.execute(
        "SELECT status FROM deliveries WHERE section=? AND date=? AND revision=?",
        (section, date, revision),
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO deliveries (section, date, revision, status, created_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            (section, date, revision, now),
        )
    # Only unsent rows are superseded. A row currently 'sending' is left to
    # settle: it reflects an in-flight transmission and must not be retried.
    conn.execute(
        "UPDATE deliveries SET status='superseded', last_error=?, claimed_at=NULL, "
        "claimed_by=NULL WHERE section=? AND date=? AND revision<? "
        "AND status IN ('pending','failed')",
        (
            "newer local revision published",
            section,
            date,
            revision,
        ),
    )


def _published_revision(
    conn: sqlite3.Connection, section: str, date: str
) -> Optional[int]:
    row = conn.execute(
        "SELECT revision FROM published WHERE section=? AND date=? LIMIT 1",
        (section, date),
    ).fetchone()
    return None if row is None else int(row["revision"])


def get_published_revision(db_path: str, section: str, date: str) -> Optional[int]:
    conn = get_conn(db_path)
    try:
        return _published_revision(conn, section, date)
    finally:
        conn.close()


def get_outbox(
    db_path: str, section: str, date: str, revision: int
) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM deliveries WHERE section=? AND date=? AND revision=?",
            (section, date, revision),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_outbox(
    db_path: str,
    limit: int = 100,
    section: Optional[str] = None,
    date: Optional[str] = None,
) -> list[dict]:
    conn = get_conn(db_path)
    try:
        where = []
        params: list = []
        if section is not None:
            where.append("section=?")
            params.append(section)
        if date is not None:
            where.append("date=?")
            params.append(date)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        params.append(limit)
        return [
            dict(r)
            for r in conn.execute(
                f"SELECT * FROM deliveries {clause} "
                "ORDER BY created_at DESC, section, date, revision DESC LIMIT ?",
                params,
            ).fetchall()
        ]
    finally:
        conn.close()


def claim_delivery(
    db_path: str, section: str, date: str, revision: int, worker: str
) -> Optional[int]:
    """Atomically claim an unsent delivery for this worker.

    The claim is a short lease rather than an in-memory lock, so it works across
    the two web processes and expires if the claiming process dies mid-send.
    Returns the updated attempt count, or ``None`` when the row is already
    sent/claimed/superseded or when the published content has moved on to a
    different revision (a stale queue row must never be transmitted).
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            published_rev = _published_revision(conn, section, date)
            if published_rev != revision:
                conn.execute("ROLLBACK")
                return None
            now = datetime.now(timezone.utc)
            stale_before = (now - timedelta(seconds=DELIVERY_LEASE_SECONDS)).isoformat(
                timespec="seconds"
            )
            row = conn.execute(
                "SELECT status, claimed_at, attempts FROM deliveries "
                "WHERE section=? AND date=? AND revision=?",
                (section, date, revision),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return None
            claimable = row["status"] in ("pending", "failed", "rejected") or (
                row["status"] == "sending"
                and (row["claimed_at"] is None or row["claimed_at"] < stale_before)
            )
            if not claimable:
                conn.execute("ROLLBACK")
                return None
            attempts = int(row["attempts"]) + 1
            cur = conn.execute(
                "UPDATE deliveries SET status='sending', attempts=?, "
                "last_attempt_at=?, claimed_at=?, claimed_by=? "
                "WHERE section=? AND date=? AND revision=? AND status=?",
                (
                    attempts,
                    now.isoformat(timespec="seconds"),
                    now.isoformat(timespec="seconds"),
                    worker,
                    section,
                    date,
                    revision,
                    row["status"],
                ),
            )
            if cur.rowcount != 1:
                conn.execute("ROLLBACK")
                return None
            conn.execute("COMMIT")
            return attempts
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def mark_delivery_sent(
    db_path: str,
    section: str,
    date: str,
    revision: int,
    worker: str,
    attempts: int,
    remote: Optional[dict] = None,
) -> None:
    """Record a successful send. Only touches the outbox, never ``published``."""
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        now = utc_now()
        conn.execute(
            "UPDATE deliveries SET status='sent', attempts=?, last_error=NULL, "
            "last_attempt_at=?, sent_at=?, sent_by=?, remote_result=?, "
            "claimed_at=NULL, claimed_by=NULL "
            "WHERE section=? AND date=? AND revision=?",
            (
                attempts,
                now,
                now,
                worker,
                None if remote is None else json.dumps(remote, sort_keys=True),
                section,
                date,
                revision,
            ),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def mark_delivery_failed(
    db_path: str,
    section: str,
    date: str,
    revision: int,
    worker: str,
    attempts: int,
    error: str,
    rejected: bool = False,
) -> None:
    """Record a failed send. The published day stays exactly as it was."""
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE deliveries SET status=?, attempts=?, last_error=?, "
            "last_attempt_at=?, claimed_at=NULL, claimed_by=NULL "
            "WHERE section=? AND date=? AND revision=?",
            (
                "rejected" if rejected else "failed",
                attempts,
                error[:500],
                utc_now(),
                section,
                date,
                revision,
            ),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def pending_deliveries(db_path: str, limit: int = 20) -> list[dict]:
    """Deliveries the background sender should attempt, oldest first.

    Only the newest locally published revision of each day is eligible:
    anything older is superseded and must never be shown as current.
    """
    conn = get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT d.* FROM deliveries d "
            "JOIN (SELECT section, date, MAX(revision) AS max_rev "
            "      FROM deliveries GROUP BY section, date) m "
            "  ON d.section=m.section AND d.date=m.date AND d.revision=m.max_rev "
            "WHERE d.status IN ('pending','failed') "
            "ORDER BY d.created_at, d.section, d.date LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def ensure_delivery(db_path: str, section: str, date: str, revision: int) -> dict:
    """Make sure a queue row exists for the current published revision.

    Covers databases published before the outbox existed (or a row lost by an
    operator). Idempotent: an existing row keeps its status/history.
    """
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM deliveries WHERE section=? AND date=? AND revision=?",
            (section, date, revision),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO deliveries (section, date, revision, status, created_at) "
                "VALUES (?, ?, ?, 'pending', ?)",
                (section, date, revision, utc_now()),
            )
            row = conn.execute(
                "SELECT * FROM deliveries WHERE section=? AND date=? AND revision=?",
                (section, date, revision),
            ).fetchone()
        conn.execute("COMMIT")
        return dict(row)
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def retry_delivery(db_path: str, section: str, date: str, revision: int) -> dict:
    """Reset a failed/rejected delivery to pending for a manual retry."""
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM deliveries WHERE section=? AND date=? AND revision=?",
            (section, date, revision),
        ).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            raise DeliveryNotFoundError(
                f"no delivery for {section} {date} revision {revision}"
            )
        published_rev = _published_revision(conn, section, date)
        if published_rev != revision:
            conn.execute("ROLLBACK")
            raise StaleRevisionError(
                f"revision {revision} is no longer the locally published "
                f"revision ({published_rev}) for {section} {date}"
            )
        if row["status"] == "sent":
            conn.execute("ROLLBACK")
            return dict(row)
        if row["status"] == "sending" and row["claimed_at"] and row["claimed_at"] >= (
            datetime.now(timezone.utc) - timedelta(seconds=DELIVERY_LEASE_SECONDS)
        ).isoformat(timespec="seconds"):
            # Another web instance is transmitting right now. Don't double-send;
            # let its own settle record the result.
            conn.execute("ROLLBACK")
            return dict(row)
        conn.execute(
            "UPDATE deliveries SET status='pending', claimed_at=NULL, "
            "claimed_by=NULL WHERE section=? AND date=? AND revision=?",
            (section, date, revision),
        )
        conn.execute("COMMIT")
        return dict(row)
    finally:
        conn.close()


def release_worker_claims(db_path: str, worker: str) -> int:
    """On startup: a restarted worker cannot be mid-send, so release its leases.

    Without this a process killed between claiming and settling would hold the
    lease for ``DELIVERY_LEASE_SECONDS`` while the other instance waits.
    """
    conn = get_conn(db_path)
    try:
        cur = conn.execute(
            "UPDATE deliveries SET status='failed', "
            "last_error='worker restarted before delivery finished', "
            "claimed_at=NULL, claimed_by=NULL "
            "WHERE status='sending' AND claimed_by=?",
            (worker,),
        )
        return cur.rowcount
    finally:
        conn.close()


def prune_delivery_claims(db_path: str) -> int:
    """Release expired send leases (worker killed mid-send) back to failed."""
    conn = get_conn(db_path)
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=DELIVERY_LEASE_SECONDS)
        ).isoformat(timespec="seconds")
        cur = conn.execute(
            "UPDATE deliveries SET status='failed', "
            "last_error='send lease expired; worker likely restarted', "
            "claimed_at=NULL, claimed_by=NULL "
            "WHERE status='sending' AND (claimed_at IS NULL OR claimed_at<?)",
            (cutoff,),
        )
        return cur.rowcount
    finally:
        conn.close()


def get_run(db_path: str, run_id: int) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_runs(db_path: str, section: str, date: str) -> list[dict]:
    conn = get_conn(db_path)
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM runs WHERE section=? AND date=? ORDER BY id DESC",
                (section, date),
            ).fetchall()
        ]
    finally:
        conn.close()


def get_published(db_path: str, section: str, date: str) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT time, nh3n, source_page, revision, published_at, published_by "
            "FROM published WHERE section=? AND date=? ORDER BY time",
            (section, date),
        ).fetchall()
        if not rows:
            return None
        first = rows[0]
        return {
            "section": section,
            "date": date,
            "revision": first["revision"],
            "published_at": first["published_at"],
            "published_by": first["published_by"],
            "rows": [
                {
                    "time": r["time"],
                    "nh3n": r["nh3n"],
                    "source_page": r["source_page"],
                    "revision": r["revision"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()
