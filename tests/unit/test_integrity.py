#!/usr/bin/env python3
"""
Unit tests for integrity.py module.
"""

from mailbackup.integrity import rebuild_docset


class TestRebuildDocset:
    """Tests for rebuild_docset function."""

    def test_rebuild_docset_basic(self, test_settings):
        """Test rebuilding a basic docset."""
        # Create test email file
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_path),
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        result = rebuild_docset(test_settings, 2024, "test_folder", row)

        assert result.exists()
        assert (result / "email.eml").exists()
        assert (result / "info.json").exists()

    def test_rebuild_docset_with_attachments(self, test_settings):
        """Test rebuilding docset with attachments."""
        # Create test email and attachment
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        test_settings.attachments_dir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        attach_path = test_settings.attachments_dir / "2024" / "test.pdf"
        attach_path.parent.mkdir(parents=True, exist_ok=True)
        attach_path.write_bytes(b"PDF content")

        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_path),
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": f'["{attach_path}"]',
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        result = rebuild_docset(test_settings, 2024, "test_folder", row)

        assert result.exists()
        assert (result / "email.eml").exists()
        assert (result / "info.json").exists()
        # Attachment should be copied with sanitized name
        assert any(f.name.endswith(".pdf") for f in result.iterdir())

    def test_rebuild_docset_missing_email(self, test_settings):
        """Test rebuilding docset when email file is missing."""
        row = {
            "id": 1,
            "hash": "abc123",
            "path": "/nonexistent/test.eml",
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        result = rebuild_docset(test_settings, 2024, "test_folder", row)

        assert result.exists()
        # Should still create info.json
        assert (result / "info.json").exists()
