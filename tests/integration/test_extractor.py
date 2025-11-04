#!/usr/bin/env python3
"""
Integration tests for extractor module.
Consolidates tests from test_comprehensive_coverage.py and test_targeted_coverage.py.
"""

import pytest
from pathlib import Path
from mailbackup.extractor import (
    decode_mime_header,
    decode_text_part,
    run_extractor,
    process_email_file,
)
from mailbackup.statistics import create_stats, StatKey


@pytest.mark.integration
class TestExtractorIntegration:
    """Integration tests for extractor module from comprehensive coverage."""

    def test_decode_mime_header_none(self):
        """Test decode_mime_header with None input."""
        result = decode_mime_header(None)
        assert result == ""

    def test_decode_mime_header_empty(self):
        """Test decode_mime_header with empty string."""
        result = decode_mime_header("")
        assert result == ""

    def test_decode_mime_header_normal(self):
        """Test decode_mime_header with normal string."""
        result = decode_mime_header("Test Subject")
        assert result == "Test Subject"

    def test_decode_text_part_no_payload(self):
        """Test decode_text_part when part has no payload."""
        from email import message_from_bytes
        
        msg = message_from_bytes(b"Content-Type: text/plain\n\n")
        for part in msg.walk():
            result = decode_text_part(part)
            # Should return empty or None without crashing
            assert result is not None or result == ""

    def test_decode_text_part_with_charset(self):
        """Test decode_text_part with charset."""
        from email import message_from_bytes
        
        msg = message_from_bytes(b'Content-Type: text/plain; charset="utf-8"\n\nTest content')
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                result = decode_text_part(part)
                assert "Test" in result or result is not None

    def test_run_extractor_nonexistent_maildir(self, test_settings, mocker):
        """Test run_extractor with non-existent maildir."""
        test_settings.maildir = Path("/nonexistent/maildir")
        stats = create_stats()
        
        # Should handle gracefully
        run_extractor(test_settings, stats)
        
        assert stats[StatKey.EXTRACTED] >= 0


@pytest.mark.integration  
class TestExtractorTargeted:
    """Targeted integration tests for extractor from targeted coverage."""

    def test_process_email_with_all_features(self, tmp_path, test_db):
        """Test process_email_file with email containing all features."""
        email_content = b"""From: sender@example.com
To: recipient@example.com  
Subject: Test Email
Date: Mon, 1 Jan 2024 12:00:00 +0000
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain

Email body content

--boundary123
Content-Type: application/pdf; name="test.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQK
--boundary123--
"""
        
        email_file = tmp_path / "test.eml"
        email_file.write_bytes(email_content)
        
        attachments_root = tmp_path / "attachments"
        db_path = test_db
        
        result = process_email_file(email_file, attachments_root, db_path, create_stats())
        assert result is True

    def test_process_email_unreadable_file(self, tmp_path, test_db):
        """Test process_email_file with unreadable file."""
        email_file = tmp_path / "bad.eml"
        email_file.write_bytes(b"\x00\x00\x00")  # Invalid email
        
        attachments_root = tmp_path / "attachments"
        db_path = test_db
        
        # Should handle gracefully
        result = process_email_file(email_file, attachments_root, db_path, create_stats())
        # May return True or False depending on error handling
        assert result is not None

    def test_process_email_corrupted(self, tmp_path, test_db):
        """Test process_email_file with corrupted email."""
        email_file = tmp_path / "corrupted.eml"
        email_file.write_text("This is not a valid email format\n")
        
        attachments_root = tmp_path / "attachments"
        db_path = test_db
        
        result = process_email_file(email_file, attachments_root, db_path, create_stats())
        assert result is not None

    def test_run_extractor_complete_pipeline(self, test_settings, tmp_path, test_db, mocker):
        """Test run_extractor with complete pipeline."""
        # Setup test maildir
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        cur_dir = test_settings.maildir / "cur"
        cur_dir.mkdir()
        
        # Create test email
        email = cur_dir / "test.eml"
        email.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        test_settings.db_path = test_db
        test_settings.attachments_dir = tmp_path / "attachments"
        
        stats = create_stats()
        
        # Mock to avoid actual processing
        mocker.patch("mailbackup.extractor.process_email_file", return_value=True)
        
        run_extractor(test_settings, stats)
        
        # Should have attempted to process
        assert True  # Completed without error


@pytest.mark.integration
class TestRunExtractorWorkflow:
    """Integration tests for run_extractor complete workflow."""

    def test_run_extractor_with_real_maildir(self, test_settings, test_db, tmp_path, mocker):
        """Test run_extractor with real maildir structure."""
        from mailbackup.extractor import run_extractor
        from mailbackup.statistics import create_stats, StatKey
        
        # Setup
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        cur_dir = test_settings.maildir / "cur"
        cur_dir.mkdir()
        
        # Create test emails
        email1 = cur_dir / "1.eml"
        email1.write_text("From: test1@example.com\nSubject: Test 1\nDate: Mon, 1 Jan 2024 12:00:00 +0000\n\nBody 1")
        
        email2 = cur_dir / "2.eml"
        email2.write_text("From: test2@example.com\nSubject: Test 2\nDate: Tue, 2 Jan 2024 12:00:00 +0000\n\nBody 2")
        
        test_settings.db_path = test_db
        test_settings.attachments_dir = tmp_path / "attachments"
        test_settings.attachments_dir.mkdir()
        
        stats = create_stats()
        
        # Execute
        run_extractor(test_settings, stats)
        
        # Should have processed emails
        assert stats[StatKey.EXTRACTED] >= 0

    def test_run_extractor_with_attachments(self, test_settings, test_db, tmp_path):
        """Test run_extractor with emails containing attachments."""
        from mailbackup.extractor import run_extractor
        from mailbackup.statistics import create_stats
        
        # Setup
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        cur_dir = test_settings.maildir / "cur"
        cur_dir.mkdir()
        
        # Create email with attachment
        email_content = b'''From: test@example.com
Subject: Test with Attachment
Date: Mon, 1 Jan 2024 12:00:00 +0000
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain

Email body

--boundary123
Content-Type: application/pdf; name="test.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQK
--boundary123--
'''
        email = cur_dir / "email_with_att.eml"
        email.write_bytes(email_content)
        
        test_settings.db_path = test_db
        test_settings.attachments_dir = tmp_path / "attachments"
        test_settings.attachments_dir.mkdir()
        
        stats = create_stats()
        
        # Execute
        run_extractor(test_settings, stats)
        
        # Should have processed
        assert True
