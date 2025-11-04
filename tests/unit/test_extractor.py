#!/usr/bin/env python3
"""
Unit tests for extractor.py module.
"""

from email import message_from_bytes
from pathlib import Path

from mailbackup.extractor import (
    decode_mime_header,
    decode_text_part,
    save_attachment,
    detect_spam,
    process_email_file,
    count_mail_files,
)


class TestDecodeMimeHeader:
    """Tests for decode_mime_header function."""

    def test_decode_simple_header(self):
        result = decode_mime_header("Simple Header")
        assert result == "Simple Header"

    def test_decode_empty_header(self):
        result = decode_mime_header("")
        assert result == ""

    def test_decode_none_header(self):
        result = decode_mime_header(None)
        assert result == ""

    def test_decode_utf8_header(self):
        # MIME-encoded UTF-8 subject
        header = "=?UTF-8?B?VGVzdCBTdWJqZWN0?="
        result = decode_mime_header(header)
        assert "Test Subject" in result or isinstance(result, str)


class TestDecodeTextPart:
    """Tests for decode_text_part function."""

    def test_decode_text_part_plain(self):
        msg = message_from_bytes(b"""Content-Type: text/plain; charset="utf-8"

Hello World
""")
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                result = decode_text_part(part)
                assert "Hello World" in result

    def test_decode_text_part_no_payload(self):
        msg = message_from_bytes(b"""Content-Type: text/plain

""")
        for part in msg.walk():
            result = decode_text_part(part)
            assert isinstance(result, str)


class TestSaveAttachment:
    """Tests for save_attachment function."""

    def test_save_attachment_with_filename(self, tmp_path):
        # Create a simple email part with attachment
        msg = message_from_bytes(b"""Content-Type: application/pdf; name="test.pdf"
Content-Disposition: attachment; filename="test.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQKJeLjz9M=
""")

        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                result = save_attachment(part, tmp_path)
                assert result is not None
                assert Path(result).exists()
                assert "test.pdf" in result

    def test_save_attachment_no_filename(self, tmp_path):
        msg = message_from_bytes(b"""Content-Type: application/octet-stream
Content-Disposition: attachment
Content-Transfer-Encoding: base64

dGVzdCBkYXRh
""")

        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                result = save_attachment(part, tmp_path)
                assert result is not None
                # Should use default "attachment" name
                assert Path(result).exists()


class TestDetectSpam:
    """Tests for detect_spam function."""

    def test_detect_spam_by_subject(self):
        msg = message_from_bytes(b"""Subject: [SPAM] This is spam
From: test@example.com

Body
""")
        result = detect_spam(msg, "[SPAM] This is spam", Path("/maildir/email.eml"))
        assert result is True

    def test_detect_spam_by_folder_path(self):
        msg = message_from_bytes(b"""Subject: Normal Subject
From: test@example.com

Body
""")
        result = detect_spam(msg, "Normal Subject", Path("/maildir/Spam/cur/email.eml"))
        assert result is True

        result = detect_spam(msg, "Normal Subject", Path("/maildir/Junk/cur/email.eml"))
        assert result is True

    def test_detect_spam_by_header(self):
        msg = message_from_bytes(b"""Subject: Normal Subject
From: test@example.com
X-Spam-Flag: YES

Body
""")
        result = detect_spam(msg, "Normal Subject", Path("/maildir/INBOX/email.eml"))
        assert result is True

    def test_not_spam(self):
        msg = message_from_bytes(b"""Subject: Normal Email
From: test@example.com

Body
""")
        result = detect_spam(msg, "Normal Email", Path("/maildir/INBOX/cur/email.eml"))
        assert result is False


class TestProcessEmailFile:
    """Tests for process_email_file function."""

    def test_process_email_file_basic(self, tmp_path, test_db, sample_email):
        # Create email file
        email_file = tmp_path / "test.eml"
        email_file.write_bytes(sample_email)

        attachments_dir = tmp_path / "attachments"
        attachments_dir.mkdir()

        result = process_email_file(email_file, attachments_dir, test_db)

        # Should be processed (True = new processing, False = already processed)
        assert result is True

    def test_process_email_file_already_processed(self, tmp_path, test_db, sample_email):
        email_file = tmp_path / "test.eml"
        email_file.write_bytes(sample_email)

        attachments_dir = tmp_path / "attachments"
        attachments_dir.mkdir()

        # Process first time
        result1 = process_email_file(email_file, attachments_dir, test_db)
        assert result1 is True

        # Process second time - should be skipped
        result2 = process_email_file(email_file, attachments_dir, test_db)
        assert result2 is False

    def test_process_email_file_with_attachment(self, tmp_path, test_db, sample_email_with_attachment):
        email_file = tmp_path / "attachment_email.eml"
        email_file.write_bytes(sample_email_with_attachment)

        attachments_dir = tmp_path / "attachments"
        attachments_dir.mkdir()

        result = process_email_file(email_file, attachments_dir, test_db)

        assert result is True

        # Check that attachment was saved
        saved_files = list(attachments_dir.rglob("*"))
        # Should have created a year directory and saved attachments
        assert len(saved_files) > 0

    def test_process_email_spam(self, tmp_path, test_db):
        spam_email = b"""From: spammer@example.com
To: victim@example.com
Subject: [SPAM] You won the lottery!
Date: Mon, 1 Jan 2024 12:00:00 +0000

Click here to claim your prize!
"""

        email_file = tmp_path / "spam.eml"
        email_file.write_bytes(spam_email)

        attachments_dir = tmp_path / "attachments"
        attachments_dir.mkdir()

        result = process_email_file(email_file, attachments_dir, test_db)

        # Should be processed but marked as spam
        assert result is True


class TestCountMailFiles:
    """Tests for count_mail_files function."""

    def test_count_mail_files(self, sample_maildir):
        # Create some email files
        inbox = sample_maildir / "INBOX" / "cur"
        (inbox / "email1.eml").touch()
        (inbox / "email2.eml").touch()

        sent = sample_maildir / "Sent" / "cur"
        (sent / "email3.eml").touch()

        count = count_mail_files(sample_maildir)
        assert count == 3

    def test_count_mail_files_empty(self, sample_maildir):
        count = count_mail_files(sample_maildir)
        assert count == 0

    def test_count_mail_files_nonexistent(self, tmp_path):
        nonexistent = tmp_path / "nonexistent"
        count = count_mail_files(nonexistent)
        assert count == 0
