"""SQLite archive of complete daily observation files at the receiving end.

Rules enforced here:

* Only complete days are accepted and ever served: exactly 24 unique hourly
  UTC slots, row count and content digest must match the payload.
* Idempotent per version: re-sending the same (section, date, revision) with
  identical content acknowledges ``duplicate`` and writes nothing.
* A late older revision can never overwrite a newer one (``stale_revision``).
  The same revision with different content is a permanent ``conflict``.
* Each day is replaced atomically in one ``BEGIN IMMEDIATE`` transaction, so a
  reader never sees a half-written file.

Standard library only, shared by the receiver service and test harness.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

EXPECTED_HOURS = {f"{h:02d}:00" for h in range(24)}

SCHEMA = """
CREATE TABLE IF NOT EXISTS days (
    section       TEXT NOT NULL,
    date          TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    row_count     INTEGER NOT NULL,
    digest        TEXT NOT NULL,
    published_at  TEXT,
    published_by  TEXT,
    delivered_by  TEXT,
    received_at   TEXT NOT NULL,
    PRIMARY KEY (section, date)
);

CREATE TABLE IF NOT EXISTS day_rows (
    section     TEXT NOT NULL,
    date        TEXT NOT NULL,
    time        TEXT NOT NULL,
    nh3n        REAL NOT NULL,
    source_page INTEGER NOT NULL,
    PRIMARY KEY (section, date, time),
    FOREIGN KEY (section, date) REFERENCES days (section, date) ON DELETE CASCADE
);

-- Audit of every delivery attempt that reached the receiving end.
CREATE TABLE IF NOT EXISTS deliveries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    section     TEXT NOT NULL,
    date        TEXT NOT NULL,
    revision    INTEGER NOT NULL,
    digest      TEXT,
    outcome     TEXT NOT NULL,
    detail      TEXT,
    remote_addr TEXT,
    received_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_receiver_deliveries
    ON deliveries (received_at);
"""


class DeliveryRejected(Exception):
    def __init__(self, error: str, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.error = error
        self.message = message
        self.status_code = status_code


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def content_digest(section: str, date: str, revision: int, rows: list[dict]) -> str:
    """Must match ``web.app.db.delivery_digest`` byte for byte."""
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


def _validate_payload(payload: dict) -> tuple[str, str, int, list[dict], str]:
    if not isinstance(payload, dict):
        raise DeliveryRejected("invalid_payload", "body must be a JSON object")
    section = payload.get("section")
    date = payload.get("date")
    revision = payload.get("revision")
    rows = payload.get("rows")

    if not isinstance(section, str) or not section.strip():
        raise DeliveryRejected("invalid_payload", "section is required")
    if not isinstance(date, str) or len(date) != 10 or date[4] != "-" or date[7] != "-":
        raise DeliveryRejected("invalid_payload", "date must be YYYY-MM-DD")
    if not isinstance(revision, int) or revision < 1:
        raise DeliveryRejected("invalid_payload", "revision must be a positive integer")
    if payload.get("complete") is not True:
        raise DeliveryRejected(
            "incomplete_day", "only complete daily files are accepted"
        )
    if not isinstance(rows, list):
        raise DeliveryRejected("invalid_payload", "rows must be a list")
    if payload.get("row_count") != len(rows):
        raise DeliveryRejected(
            "incomplete_day",
            f"row_count {payload.get('row_count')} != {len(rows)} rows in payload",
        )

    clean: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise DeliveryRejected("invalid_payload", "each row must be an object")
        t = row.get("time")
        if not isinstance(t, str) or t not in EXPECTED_HOURS:
            raise DeliveryRejected(
                "incomplete_day", f"row time {t!r} is not an hourly UTC slot"
            )
        if t in seen:
            raise DeliveryRejected("incomplete_day", f"duplicate hour {t}")
        seen.add(t)
        value = row.get("nh3n")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DeliveryRejected("invalid_payload", f"row {t} has non-numeric nh3n")
        source_page = row.get("source_page")
        if not isinstance(source_page, int) or source_page < 1:
            raise DeliveryRejected(
                "invalid_payload", f"row {t} is missing a valid source_page"
            )
        clean.append(
            {"time": t, "nh3n": float(value), "source_page": int(source_page)}
        )

    if seen != EXPECTED_HOURS:
        missing = sorted(EXPECTED_HOURS - seen)
        raise DeliveryRejected(
            "incomplete_day",
            f"day is missing hourly slots: {', '.join(missing)}",
        )

    digest = payload.get("digest")
    computed = content_digest(section, date, revision, clean)
    if not isinstance(digest, str) or digest != computed:
        raise DeliveryRejected(
            "digest_mismatch",
            "payload digest does not match its contents",
        )
    return section, date, int(revision), clean, computed


def _audit(
    conn: sqlite3.Connection,
    section: str,
    date: str,
    revision: int,
    digest: Optional[str],
    outcome: str,
    detail: Optional[str],
    remote_addr: Optional[str],
) -> None:
    conn.execute(
        "INSERT INTO deliveries (section, date, revision, digest, outcome, detail, "
        "remote_addr, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (section, date, revision, digest, outcome, detail, remote_addr, utc_now()),
    )


def receive_day(
    db_path: str, payload: dict, remote_addr: Optional[str] = None
) -> dict:
    """Accept one delivery, atomically. Returns an acknowledgment dict.

    Outcomes: ``created``, ``replaced``, ``duplicate``; errors raise
    ``DeliveryRejected``.
    """
    section, date, revision, rows, digest = _validate_payload(payload)
    published_at = payload.get("published_at")
    published_by = payload.get("published_by")
    delivered_by = payload.get("delivered_by")

    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT revision, digest, row_count, received_at FROM days "
                "WHERE section=? AND date=?",
                (section, date),
            ).fetchone()

            if existing is not None:
                current = int(existing["revision"])
                if revision < current:
                    _audit(conn, section, date, revision, digest, "rejected_stale",
                           f"current revision {current}", remote_addr)
                    conn.execute("COMMIT")
                    raise DeliveryRejected(
                        "stale_revision",
                        f"revision {revision} is older than stored revision {current}",
                        status_code=409,
                    )
                if revision == current:
                    if digest != existing["digest"]:
                        _audit(conn, section, date, revision, digest, "rejected_conflict",
                               "same revision with different content", remote_addr)
                        conn.execute("COMMIT")
                        raise DeliveryRejected(
                            "revision_conflict",
                            f"revision {revision} was already stored with different "
                            "content",
                            status_code=409,
                        )
                    # Same version, same bytes: acknowledge, write nothing.
                    _audit(conn, section, date, revision, digest, "duplicate", None,
                           remote_addr)
                    conn.execute("COMMIT")
                    return {
                        "status": "duplicate",
                        "section": section,
                        "date": date,
                        "revision": revision,
                        "rows": existing["row_count"],
                        "received_at": existing["received_at"],
                    }

            now = utc_now()
            if existing is not None:
                conn.execute(
                    "DELETE FROM day_rows WHERE section=? AND date=?", (section, date)
                )
            conn.execute(
                "INSERT INTO days (section, date, revision, row_count, digest, "
                "published_at, published_by, delivered_by, received_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(section, date) DO UPDATE SET revision=excluded.revision, "
                "row_count=excluded.row_count, digest=excluded.digest, "
                "published_at=excluded.published_at, published_by=excluded.published_by, "
                "delivered_by=excluded.delivered_by, received_at=excluded.received_at",
                (section, date, revision, len(rows), digest, published_at,
                 published_by, delivered_by, now),
            )
            conn.executemany(
                "INSERT INTO day_rows (section, date, time, nh3n, source_page) "
                "VALUES (?, ?, ?, ?, ?)",
                [(section, date, r["time"], r["nh3n"], r["source_page"]) for r in rows],
            )
            _audit(conn, section, date, revision, digest,
                   "replaced" if existing is not None else "created", None, remote_addr)
            conn.execute("COMMIT")
            return {
                "status": "replaced" if existing is not None else "created",
                "section": section,
                "date": date,
                "revision": revision,
                "rows": len(rows),
                "received_at": now,
            }
        except DeliveryRejected:
            raise
        except Exception:
            conn.execute("ROLLBACK")
            raise
    except DeliveryRejected:
        # Already committed its audit row (or never opened a write txn).
        raise
    finally:
        conn.close()


def get_day(db_path: str, section: str, date: str) -> Optional[dict]:
    conn = get_conn(db_path)
    try:
        meta = conn.execute(
            "SELECT * FROM days WHERE section=? AND date=?", (section, date)
        ).fetchone()
        if meta is None:
            return None
        rows = conn.execute(
            "SELECT time, nh3n, source_page FROM day_rows "
            "WHERE section=? AND date=? ORDER BY time",
            (section, date),
        ).fetchall()
        # Defensive completeness check: never serve a partial file.
        if len(rows) != int(meta["row_count"]) or len(rows) != len(EXPECTED_HOURS):
            return None
        return {
            "section": meta["section"],
            "date": meta["date"],
            "revision": int(meta["revision"]),
            "complete": True,
            "row_count": len(rows),
            "digest": meta["digest"],
            "published_at": meta["published_at"],
            "published_by": meta["published_by"],
            "delivered_by": meta["delivered_by"],
            "received_at": meta["received_at"],
            "rows": [
                {
                    "time": r["time"],
                    "nh3n": r["nh3n"],
                    "source_page": r["source_page"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


def list_days(db_path: str, limit: int = 200) -> list[dict]:
    conn = get_conn(db_path)
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT section, date, revision, row_count, digest, received_at "
                "FROM days ORDER BY date DESC, section LIMIT ?",
                (limit,),
            ).fetchall()
        ]
    finally:
        conn.close()


def list_delivery_audit(db_path: str, limit: int = 100) -> list[dict]:
    conn = get_conn(db_path)
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM deliveries ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        ]
    finally:
        conn.close()
