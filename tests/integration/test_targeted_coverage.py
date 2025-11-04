#!/usr/bin/env python3
"""
Targeted integration tests to reach 94% coverage.
Focuses on extractor and integrity modules.
"""

import email
from mailbackup.statistics import StatKey, create_stats
import shutil
from mailbackup.statistics import StatKey, create_stats
from pathlib import Path
from unittest.mock import Mock

import pytest
from mailbackup.statistics import StatKey, create_stats

from mailbackup import db
from mailbackup.extractor import (
    run_extractor,
    process_email_file,
    detect_spam,
    save_attachment,
    decode_text_part,
    count_mail_files,
    iter_mail_files,
)
from mailbackup.integrity import (
    rebuild_docset,
    integrity_check,
)
from mailbackup.manifest import ManifestManager


@pytest.mark.integration
class TestExtractorTargeted:
    """Targeted tests for extractor.py to improve coverage."""

    def test_process_email_with_all_features(self, test_settings, tmp_path, sample_email_with_attachment):
        """Test process_email_file with attachments and bodies."""
        email_file = tmp_path / "email.eml"
        email_file.write_bytes(sample_email_with_attachment)

        attachments_root = tmp_path / "attachments"
        db_path = tmp_path / "test.db"
        db.ensure_schema(db_path)

        result = process_email_file(email_file, attachments_root, db_path)

        assert result is True
        assert attachments_root.exists()

    def test_process_email_unreadable_file(self, tmp_path):
        """Test process_email_file with unreadable file."""
        email_file = tmp_path / "nonexistent.eml"
        attachments_root = tmp_path / "attachments"
        db_path = tmp_path / "test.db"
        db.ensure_schema(db_path)

        result = process_email_file(email_file, attachments_root, db_path)
        assert result is False

    def test_process_email_corrupted(self, tmp_path):
        """Test process_email_file with corrupted email."""
        email_file = tmp_path / "bad.eml"
        email_file.write_bytes(b"\xFF\xFE\xFD invalid")

        attachments_root = tmp_path / "attachments"
        db_path = tmp_path / "test.db"
        db.ensure_schema(db_path)

        result = process_email_file(email_file, attachments_root, db_path)
        # May or may not succeed, just ensure no crash
        assert result in (True, False)

    def test_detect_spam_subject(self):
        """Test detect_spam by subject keywords."""
        msg = email.message_from_bytes(b"Subject: [SPAM] Test\n\nBody")
        result = detect_spam(msg, "[spam] test", Path("/inbox/email.eml"))
        assert result is True

    def test_detect_spam_folder(self):
        """Test detect_spam by folder path."""
        msg = email.message_from_bytes(b"Subject: Test\n\nBody")
        result = detect_spam(msg, "test", Path("/home/user/Maildir/Spam/cur/email.eml"))
        assert result is True

    def test_detect_spam_header_flag(self):
        """Test detect_spam by X-Spam-Flag header."""
        msg = email.message_from_bytes(b"Subject: Test\nX-Spam-Flag: YES\n\nBody")
        result = detect_spam(msg, "test", Path("/inbox/email.eml"))
        assert result is True

    def test_detect_spam_header_status(self):
        """Test detect_spam by X-Spam-Status header."""
        msg = email.message_from_bytes(b"Subject: Test\nX-Spam-Status: Yes, score=5.0\n\nBody")
        result = detect_spam(msg, "test", Path("/inbox/email.eml"))
        assert result is True

    def test_detect_not_spam(self):
        """Test legitimate email is not marked as spam."""
        msg = email.message_from_bytes(b"Subject: Legitimate\n\nBody")
        result = detect_spam(msg, "legitimate", Path("/inbox/email.eml"))
        assert result is False

    def test_save_attachment_success(self, tmp_path):
        """Test save_attachment successfully saves file."""
        msg = email.message.EmailMessage()
        msg.set_content("Test attachment content", filename="test.txt")

        for part in msg.walk():
            if part.get_filename():
                result = save_attachment(part, tmp_path)
                assert result is not None
                assert Path(result).exists()

    def test_save_attachment_no_payload(self, tmp_path):
        """Test save_attachment with no payload."""
        part = email.message.EmailMessage()
        part.set_type("application/pdf")
        part.add_header("Content-Disposition", "attachment", filename="test.pdf")

        result = save_attachment(part, tmp_path)
        assert result is None

    def test_decode_text_part_charset_utf8(self):
        """Test decode_text_part with UTF-8 charset."""
        part = email.message.EmailMessage()
        part.set_content("Test content", charset="utf-8")

        result = decode_text_part(part)
        assert "Test" in result

    def test_decode_text_part_invalid_charset_fallback(self):
        """Test decode_text_part falls back to UTF-8 for invalid charset."""
        part = email.message.EmailMessage()
        part.set_payload("Test".encode("utf-8"), charset="invalid-charset")

        result = decode_text_part(part)
        # Should handle gracefully
        assert isinstance(result, str)

    def test_count_mail_files_existing_dir(self, tmp_path):
        """Test count_mail_files with existing directory."""
        maildir = tmp_path / "maildir"
        maildir.mkdir()
        # Create account1 with nested folder structure
        account1 = maildir / "account1"
        account1.mkdir()
        folder1 = account1 / "INBOX"
        folder1.mkdir()
        (folder1 / "cur").mkdir()
        (folder1 / "cur" / "email1.eml").write_text("test")
        (folder1 / "tmp").mkdir()
        (folder1 / "tmp" / "email-1.eml").write_text("test")  # Should be ignored
        (folder1 / "new").mkdir()
        (folder1 / "new" / "email2.eml").write_text("test")
        
        # Create account2 with direct cur/new folders
        account2 = maildir / "account2"
        account2.mkdir()
        (account2 / "cur").mkdir()
        (account2 / "cur" / "email3.eml").write_text("test")
        (account2 / "tmp").mkdir()
        (account2 / "tmp" / "email-2.eml").write_text("test")  # Should be ignored
        (account2 / "new").mkdir()
        (account2 / "new" / "email4.eml").write_text("test")

        count = count_mail_files(maildir)
        assert count == 4

    def test_count_mail_files_nonexistent_dir(self, tmp_path):
        """Test count_mail_files with nonexistent directory."""
        count = count_mail_files(tmp_path / "nonexistent")
        assert count == 0

    def test_iter_mail_files(self, tmp_path):
        """Test iter_mail_files yields all files."""
        maildir = tmp_path / "maildir"
        maildir.mkdir()
        # Create account1 with nested folder structure
        account1 = maildir / "account1"
        account1.mkdir()
        folder1 = account1 / "INBOX"
        folder1.mkdir()
        (folder1 / "cur").mkdir()
        (folder1 / "cur" / "email1.eml").write_text("test")
        (folder1 / "tmp").mkdir()
        (folder1 / "tmp" / "wrong").write_text("test")  # Should be ignored
        (folder1 / "new").mkdir()
        (folder1 / "new" / "email2.eml").write_text("test")
        
        # Create account2 with direct cur/new folders
        account2 = maildir / "account2"
        account2.mkdir()
        (account2 / "cur").mkdir()
        (account2 / "cur" / "email3.eml").write_text("test")
        (account2 / "tmp").mkdir()
        (account2 / "tmp" / "wrong").write_text("test")  # Should be ignored
        (account2 / "new").mkdir()
        (account2 / "new" / "email4.eml").write_text("test")

        files = list(iter_mail_files(maildir))
        assert len(files) == 4
        assert not any(file.name == "wrong" for file in files)

    def test_run_extractor_complete_pipeline(self, test_settings, tmp_path, sample_email):
        """Test complete extractor pipeline."""
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        inbox = test_settings.maildir / "INBOX" / "cur"
        inbox.mkdir(parents=True, exist_ok=True)

        for i in range(3):
            email_file = inbox / f"test{i}.eml"
            modified_email = sample_email.replace(b"Test Email", f"Test Email {i}".encode())
            email_file.write_bytes(modified_email)

        stats = {"extracted": 0}
        run_extractor(test_settings, stats)

        assert stats[StatKey.EXTRACTED] == 3


@pytest.mark.integration
class TestIntegrityTargeted:
    """Targeted tests for integrity.py to improve coverage."""

    def test_rebuild_docset_with_existing_files(self, test_settings, tmp_path):
        """Test rebuild_docset with all files existing."""
        import json

        mail_file = tmp_path / "test.eml"
        mail_file.write_text("From: test@example.com\nSubject: Test\n\nBody")

        attach_file = tmp_path / "doc.pdf"
        attach_file.write_bytes(b"PDF content")

        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(mail_file),
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": json.dumps([str(attach_file)]),
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        result = rebuild_docset(test_settings, 2024, "test_folder", row)

        assert result.exists()
        assert (result / "email.eml").exists()
        assert (result / "info.json").exists()

        attachments = [f for f in result.iterdir() if f.name not in ("email.eml", "info.json")]
        assert len(attachments) == 1

        shutil.rmtree(result, ignore_errors=True)

    def test_integrity_check_with_valid_manifest(self, test_settings, tmp_path, mocker):
        """Test integrity_check with valid manifest."""
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        manifest_file.write_text("hash1,2024/folder/email.eml\n")

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        db_row = {
            "id": 1,
            "hash": "hash1",
            "hash_sha256": "hash1",
            "path": "/path/email.eml",
            "remote_path": "2024/folder/email.eml",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])

        manifest = Mock(spec=ManifestManager)
        manifest.upload_manifest_if_needed = Mock()
        stats = {"verified": 0}

        integrity_check(test_settings, manifest, stats)

        assert stats[StatKey.VERIFIED] == 1

    def test_integrity_check_skip_no_remote_path(self, test_settings, tmp_path, mocker):
        """Test integrity_check skips entries without remote_path."""
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        manifest_file.write_text("")

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        db_row = {
            "id": 1,
            "hash": "hash1",
            "hash_sha256": "hash1",
            "path": "/path/email.eml",
            "remote_path": "",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])

        manifest = Mock(spec=ManifestManager)
        manifest.upload_manifest_if_needed = Mock()
        stats = {"verified": 0}

        integrity_check(test_settings, manifest, stats)

        assert stats[StatKey.VERIFIED] == 0
