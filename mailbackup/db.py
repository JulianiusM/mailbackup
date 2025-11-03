#!/usr/bin/env python3

"""
db.py

SQLite access layer:
- ensure schema
- fetch unsynced
- mark synced
- fetch synced
- mark archived

Uses thread-local connections to avoid cross-thread SQLite errors.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import List, Optional

from mailbackup.logger import get_logger
from mailbackup.utils import parse_year_and_ts

_thread_local = threading.local()


def get_connection(db_path: Path) -> sqlite3.Connection:
    """
    Return a sqlite3.Connection specific to the current thread and db_path.
    Ensures parent directories exist and uses check_same_thread=False for safety
    when connections are used across thread boundaries (we still keep per-thread conns).
    """
    conns = getattr(_thread_local, "conns", None)
    if conns is None:
        conns = {}
        setattr(_thread_local, "conns", conns)

    key = str(db_path.resolve())
    if key in conns:
        conn = conns[key]
        try:
            # quick liveness check (will raise if closed/corrupt)
            conn.execute("SELECT 1;")
            return conn
        except (sqlite3.ProgrammingError, sqlite3.OperationalError, sqlite3.DatabaseError):
            try:
                conn.close()
            except Exception:
                pass
            del conns[key]

    # ensure parent dir exists
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    # Create connection with timeout and allow cross-thread use for callers that may reuse it
    conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        # pragmatic DB settings for better concurrency and durability
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass
    conns[key] = conn
    return conn


def ensure_schema(db_path: Path) -> None:
    """
    Ensure the SQLite schema exists and is up-to-date.

    Creates the 'processed' table if missing and adds missing columns used by later
    versions. This function is safe to call multiple times and is idempotent.
    """
    _logger = get_logger(__name__)
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed
        (
            id
            INTEGER
            PRIMARY
            KEY
            AUTOINCREMENT,
            hash
            TEXT
            UNIQUE
            NOT
            NULL,
            path
            TEXT
            NOT
            NULL,
            from_header
            TEXT,
            subject
            TEXT,
            date_header
            DATETIME,
            attachments
            TEXT,
            spam
            INTEGER
            DEFAULT
            0,
            hash_sha256
            TEXT,
            synced_at
            DATETIME,
            archived_at
            DATETIME,
            remote_path
            TEXT,
            processed_at
            DATETIME
            DEFAULT
            CURRENT_TIMESTAMP
        );
        """
    )
    # Ensure columns exist with korrekten Typen
    cur.execute("PRAGMA table_info(processed);")
    cols = {r[1] for r in cur.fetchall()}
    type_map = {
        "synced_at": "DATETIME",
        "archived_at": "DATETIME",
        "hash_sha256": "TEXT",
        "remote_path": "TEXT",
    }
    for col, coltype in type_map.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE processed ADD COLUMN {col} {coltype};")
            _logger.debug(f"Added column {col} ({coltype}) to database.")

    # Create indices for faster lookups
    cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_synced_at ON processed(synced_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_archived_at ON processed(archived_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_date_header ON processed(date_header);")
    conn.commit()


def fetch_unsynced(db_path: Path) -> List[sqlite3.Row]:
    """
    Return a list of rows that are not yet uploaded (synced).

    Excludes messages flagged as spam. Returned rows are sqlite3.Row objects.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM processed
        WHERE (synced_at IS NULL OR synced_at = '')
          AND (spam IS NULL OR spam = 0);
        """
    )
    return cur.fetchall()


def mark_synced(db_path: Path, hash_val: Optional[str], hash_sha256: Optional[str], remote_path: Optional[str]) -> None:
    """
    Mark a processed row identified by `hash_val` as synced.

    Also stores the computed SHA256 of the email file and the remote path.
    If `hash_val` is falsy the function returns immediately.
    """
    if not hash_val:
        return
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE processed
        SET synced_at   = datetime('now'),
            hash_sha256 = ?,
            remote_path = ?
        WHERE hash = ?;
        """,
        (hash_sha256, remote_path, hash_val),
    )
    conn.commit()


def fetch_synced(db_path: Path) -> List[sqlite3.Row]:
    """
    Return rows that have been marked as synced (synced_at not null).

    Used by integrity checks to compare local metadata vs remote content.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM processed
        WHERE synced_at IS NOT NULL
          AND synced_at <> '';
        """
    )
    return cur.fetchall()


def mark_archived_year(db_path: Path, year: int) -> None:
    """
    Mark all synced items that belong to the given `year` as archived (set archived_at).

    Uses the email's date_header to determine the year. Only affects rows that
    are synced and not yet archived.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT hash, date_header FROM processed WHERE synced_at IS NOT NULL AND archived_at IS NULL;")
    rows = cur.fetchall()
    to_update = []
    for r in rows:
        y, _ = parse_year_and_ts(r["date_header"])
        if y == year:
            to_update.append(r["hash"])
    for h in to_update:
        cur.execute("UPDATE processed SET archived_at = datetime('now') WHERE hash = ?;", (h,))
    conn.commit()


# ----------------------------------------------------------------------
# New centralized helpers extracted from other modules
# ----------------------------------------------------------------------
def is_processed(db_path: Path, fingerprint: str) -> bool:
    """
    Return True if a message fingerprint already exists in the processed table.
    """
    if not fingerprint:
        return False
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed WHERE hash = ? LIMIT 1;", (fingerprint,))
    return cur.fetchone() is not None


def mark_processed(
        db_path: Path,
        fingerprint: str,
        path: str,
        from_hdr: str,
        subj: str,
        date_hdr: str,
        attachments: list[str],
        spam: bool,
) -> None:
    """
    Insert or update a processed message record.
    Uses an upsert keyed by the message fingerprint and commits.
    """
    _logger = get_logger(__name__)
    if not fingerprint:
        _logger.warning("mark_processed called without fingerprint; skipping")
        return

    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO processed
        (hash, path, from_header, subject, date_header, attachments, spam, processed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) ON CONFLICT(hash) DO
        UPDATE SET
            path=excluded.path,
            from_header=excluded.from_header,
            subject=excluded.subject,
            date_header=excluded.date_header,
            attachments=excluded.attachments,
            spam=excluded.spam,
            processed_at= CURRENT_TIMESTAMP;
        """,
        (
            fingerprint,
            path,
            from_hdr,
            subj,
            date_hdr,
            json.dumps(attachments),
            int(spam),
        ),
    )
    conn.commit()


def get_candidate_rotation_years(db_path: Path, target_year: int) -> List[int]:
    """
    Return list of years (ints) that are <= target_year and have synced emails.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT strftime('%Y', date_header) AS y
        FROM processed
        WHERE synced_at IS NOT NULL
          AND date_header IS NOT NULL
          AND strftime('%Y', date_header) <= ?
        ORDER BY 1;
        """,
        (str(target_year),),
    )
    rows = cur.fetchall()
    years = []
    for r in rows:
        if r[0] and r[0].isdigit():
            years.append(int(r[0]))
    return years


def fetch_unarchived_paths_for_year(db_path: Path, year: int) -> List[str]:
    """
    Return list of 'path' values for synced but not-yet-archived items for a given year.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT path
        FROM processed
        WHERE strftime('%Y', date_header) = ?
          AND synced_at IS NOT NULL
          AND (archived_at IS NULL OR archived_at = '');
        """,
        (str(year),),
    )
    rows = cur.fetchall()
    return [r["path"] for r in rows if r["path"]]


def update_remote_path(db_path: Path, hash_val: str, remote_path: str) -> None:
    """
    Update the remote_path for a processed row identified by hash.
    """
    if not hash_val:
        return
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE processed
        SET remote_path = ?
        WHERE hash = ?;
        """,
        (remote_path, hash_val),
    )
    conn.commit()
