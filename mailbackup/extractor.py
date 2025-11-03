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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.header import decode_header, make_header
from pathlib import Path

from . import db
from .config import Settings
from .logger import get_logger
from .utils import sanitize, StatusThread, unique_path_for_filename, sha256_bytes, parse_mail_date, parse_year_and_ts

_logger = get_logger(__name__)


# ----------------------------------------------------------------------
# MIME decoding helpers
# ----------------------------------------------------------------------


def decode_mime_header(raw_header) -> str:
    """Decode MIME-encoded email headers safely and return plain string."""
    if not raw_header:
        return ""
    try:
        decoded = make_header(decode_header(raw_header))
        if not isinstance(decoded, str):
            decoded = str(decoded)
        return decoded
    except Exception:
        try:
            return str(raw_header)
        except Exception:
            return ""


def decode_text_part(part) -> str:
    """Decode a text/plain or text/html part into UTF-8."""
    try:
        payload = part.get_payload(decode=True)
        if payload is None:
            return ""
        charset = part.get_content_charset() or "utf-8"
        try:
            return payload.decode(charset, errors="replace")
        except LookupError:
            return payload.decode("utf-8", errors="replace")
    except Exception:
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


def process_email_file(eml: Path, attachments_root: Path, db_path: Path) -> bool:
    """
    Process one email file.
    Returns True if processed (not skipped), False if already processed.
    """
    logger = get_logger(__name__)
    try:
        raw = eml.read_bytes()
    except Exception as e:
        logger.error(f"Failed to read {eml}: {e}")
        return False

    # Compute fingerprint
    fingerprint = sha256_bytes(raw)

    if db.is_processed(db_path, fingerprint):
        return False

    try:
        msg = email.message_from_bytes(raw)
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
        return True

    safe_from = sanitize(from_hdr)
    safe_subj = sanitize(subj) or "no_subject"
    year, ts = parse_year_and_ts(date_iso)
    outdir = attachments_root / str(year) / f"{safe_from}_{safe_subj}"
    outdir.mkdir(parents=True, exist_ok=True)

    saved_paths: list[str] = []

    for part in msg.walk():
        ctype = part.get_content_type()
        disp = part.get("Content-Disposition", "")

        if part.get_content_maintype() == "multipart":
            continue

        if "attachment" in disp:
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
    return True


def iter_mail_files(maildir: Path):
    """Yield email files under maildir."""
    for p in maildir.rglob("*"):
        if p.is_file():
            yield p


def count_mail_files(maildir: Path) -> int:
    """Defensive count of files in maildir."""
    if not maildir.exists():
        return 0

    return sum(1 for p in maildir.rglob("*") if p.is_file())


def run_extractor(settings: Settings, stats: dict):
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

    processed_count = 0
    processed_lock = threading.Lock()

    status_thread = StatusThread(settings.status_interval, stats)
    status_thread.start()

    def do_one(eml: Path):
        try:
            processed = process_email_file(eml, attach_dir, db_path)
            return processed, eml
        except Exception as e:
            logger.error(f"Error processing {eml}: {e}", exc_info=True)
            return False, eml

    try:
        with ThreadPoolExecutor(max_workers=settings.max_extract_workers) as ex:
            futures = [ex.submit(do_one, eml) for eml in iter_mail_files(maildir)]
            for fut in as_completed(futures):
                processed_this, eml = fut.result()
                if processed_this:
                    with processed_lock:
                        processed_count += 1
                    stats["extracted"] = stats.get("extracted", 0) + 1
                    if processed_count % 100 == 0 or processed_count == total_files:
                        remaining = total_files - processed_count
                        logger.info(
                            f"[Progress] Processed {processed_count}/{total_files} emails ({remaining} remaining)")
    finally:
        status_thread.stop()
        logger.info(f"Extraction completed: {processed_count}/{total_files} messages processed.")
