#!/usr/bin/env python3
"""
extractor.py

Integrated attachment and body extraction module for mailbackup.

Reads raw .eml files from the configured maildir, extracts attachments
and message bodies, stores them in the configured attachments directory,
and records metadata in the SQLite database.

Preserves spam detection, Windows-safe filenames, periodic status logging.
"""

from __future__ import annotations

import email
from email.header import decode_header, make_header
from pathlib import Path
from typing import Iterator

from mailbackup import db
from mailbackup.config import Settings
from mailbackup.executor import create_managed_executor
from mailbackup.logger import get_logger
from mailbackup.statistics import ThreadSafeStats, create_increment_callback, StatKey
from mailbackup.utils import (
    sanitize, unique_path_for_filename, sha256_bytes, parse_mail_date, parse_year_and_ts
)


# ----------------------------------------------------------------------
# MIME decoding helpers
# ----------------------------------------------------------------------


def decode_mime_header(raw_header) -> str:
    """Decode MIME-encoded email headers safely and return plain string."""
    logger = get_logger(__name__)
    if not raw_header:
        return ""
    try:
        decoded = make_header(decode_header(raw_header))
        if not isinstance(decoded, str):
            decoded = str(decoded)
        return decoded
    except (KeyboardInterrupt, InterruptedError):
        logger.error("Interrupted while decoding email header")
        raise
    except Exception as e:
        logger.debug(f"Failed to decode MIME-encoded header: {e}")
        return str(raw_header)


def decode_text_part(part) -> str:
    """Decode a text/plain or text/html part into UTF-8."""
    logger = get_logger(__name__)
    try:
        payload = part.get_payload(decode=True)
        if payload is None:
            return ""
        charset = part.get_content_charset() or "utf-8"
        try:
            return payload.decode(charset, errors="replace")
        except LookupError:
            return payload.decode("utf-8", errors="replace")
    except (KeyboardInterrupt, InterruptedError):
        logger.error("Interrupted while decoding email")
        raise
    except Exception as e:
        logger.debug(f"Failed to decode MIME-encoded part: {e}")
        return ""


# ----------------------------------------------------------------------
# Attachment saving
# ----------------------------------------------------------------------


def save_attachment(part, outdir: Path) -> str | None:
    """
    Save an attachment part into outdir.
    Returns the path as string, or None if saving failed.
    """
    fn = part.get_filename() or "attachment"
    fn = decode_mime_header(fn)
    fn = sanitize(fn)
    out = unique_path_for_filename(outdir, fn)

    # prefer central logger
    logger = get_logger(__name__)
    payload = part.get_payload(decode=True)
    if payload:
        try:
            out.write_bytes(payload)
            return str(out)
        except (KeyboardInterrupt, InterruptedError):
            logger.error("Interrupted while writing attachment")
            raise
        except Exception as e:
            logger.warning(f"Failed to save attachment {fn}: {e}")
    return None


# ----------------------------------------------------------------------
# Core extractor
# ----------------------------------------------------------------------


def detect_spam(msg, subj: str, eml_path: Path) -> bool:
    """Simple spam detection heuristic."""
    subject_lower = subj.lower()
    # normalisiere Pfadtrenner für plattformunabhängige Prüfung
    path_lower = str(eml_path).lower().replace("\\", "/")
    spam_flag = msg.get("X-Spam-Flag", "").lower()
    spam_status = msg.get("X-Spam-Status", "").lower()

    if any(word in subject_lower for word in ("[spam]", "***spam***", "junk", "phish")):
        return True
    elif any(folder in path_lower for folder in ("/spam/", "/junk/", "/trash/")):
        return True
    elif "yes" in spam_flag or spam_status.startswith("yes"):
        return True
    return False


def process_email_file(eml: Path, attachments_root: Path, db_path: Path, stats: ThreadSafeStats) -> bool:
    """
    Process one email file.
    Returns True if processed, False if failed.
    """
    logger = get_logger(__name__)
    try:
        raw = eml.read_bytes()
    except (KeyboardInterrupt, InterruptedError):
        logger.error("Interrupted while reading email")
        raise
    except Exception as e:
        logger.error(f"Failed to read {eml}: {e}")
        return False

    # Compute fingerprint
    fingerprint = sha256_bytes(raw)

    if db.is_processed(db_path, fingerprint):
        return True

    try:
        msg = email.message_from_bytes(raw)
    except (KeyboardInterrupt, InterruptedError):
        logger.error("Interrupted while parsing email.")
        raise
    except Exception as e:
        logger.error(f"Failed to parse email {eml}: {e}")
        return False

    from_hdr = decode_mime_header(msg.get("From", "unknown"))
    raw_date = msg.get("Date", "")
    dt = parse_mail_date(raw_date)
    date_iso = dt.isoformat()
    subj = decode_mime_header(msg.get("Subject", ""))

    # Spam detection
    if detect_spam(msg, subj, eml):
        db.mark_processed(db_path, fingerprint, str(eml), from_hdr, subj, date_iso, [], True)
        logger.info(f"Skipped spam: {eml}")
        stats.increment(StatKey.SKIPPED)
        return True

    safe_from = sanitize(from_hdr)
    safe_subj = sanitize(subj) or "no_subject"
    year, ts = parse_year_and_ts(date_iso)
    outdir = attachments_root / str(year) / f"{safe_from}_{safe_subj}"
    outdir.mkdir(parents=True, exist_ok=True)

    saved_paths: list[str] = []

    for part in msg.walk():
        ctype = part.get_content_type()
        disp = part.get("Content-Disposition")
        disp = disp if isinstance(disp, str) else str(disp or "")

        if part.get_content_maintype() == "multipart":
            continue

        if "attachment" in disp.lower():
            p = save_attachment(part, outdir)
            if p:
                saved_paths.append(p)
        elif ctype in ("text/plain", "text/html"):
            decoded_text = decode_text_part(part)
            if decoded_text.strip():
                ext = ".html" if "html" in ctype else ".txt"
                fname = "body" + ext
                outpath = unique_path_for_filename(outdir, fname)
                outpath.write_text(decoded_text, encoding="utf-8", errors="replace")
                saved_paths.append(str(outpath))

    db.mark_processed(db_path, fingerprint, str(eml), from_hdr, subj, date_iso, saved_paths, False)
    stats.increment(StatKey.EXTRACTED)
    return True


def iter_mail_files(root_maildir: Path) -> Iterator[Path]:
    """
    Yield all actual email message files under a multi-account mbsync Maildir.

    Structure assumed:
        root_maildir/
            account1/
                Folder1/{cur,new,tmp}/
                Folder2/{cur,new,tmp}/
            account2/
                INBOX/{cur,new,tmp}/
                ...
    Only files inside 'cur' and 'new' are yielded.
    """
    if not root_maildir.exists():
        return

    for account_dir in root_maildir.iterdir():
        if not account_dir.is_dir() or account_dir.name.startswith("."):
            continue

        # Walk recursively through all folders within each account
        for folder in account_dir.rglob("*"):
            if not folder.is_dir():
                continue
            if folder.name not in ("cur", "new"):
                continue

            for msg in folder.iterdir():
                if msg.is_file() and not msg.name.startswith("."):
                    yield msg


def count_mail_files(root_maildir: Path) -> int:
    """Count all email message files under a multi-account mbsync Maildir."""
    return sum(1 for _ in iter_mail_files(root_maildir))


def run_extractor(settings: Settings, stats: ThreadSafeStats):
    """Entry point for extraction stage."""
    maildir = settings.maildir
    attach_dir = settings.attachments_dir
    db_path = settings.db_path

    # Use central logger and ignore passed logger parameter
    logger = get_logger(__name__)

    if not maildir.exists():
        logger.error(f"Maildir does not exist: {maildir}")
        return

    attach_dir.mkdir(parents=True, exist_ok=True)

    # ensure_db schema uses db.ensure_schema(db_path)
    db.ensure_schema(db_path)

    total_files = count_mail_files(maildir)
    logger.info(f"Starting extraction. Total email files found: {total_files}")

    def do_one(eml: Path):
        return process_email_file(eml, attach_dir, db_path, stats)

    try:
        with create_managed_executor(
                max_workers=settings.max_extract_workers,
                name="Extractor",
                progress_interval=1000,
        ) as executor:
            results = executor.map(do_one, iter_mail_files(maildir), create_increment_callback(stats))

            # Count successfully processed emails
            processed_count = sum(1 for r in results if r.success and r.result)
    finally:
        logger.info(f"Extraction completed: {processed_count}/{total_files} messages processed.")
