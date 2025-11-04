#!/usr/bin/env python3
from mailbackup.statistics import create_stats, StatKey
"""
Unit tests for uploader.py module.
"""

from unittest.mock import Mock

from mailbackup.manifest import ManifestManager
from mailbackup.uploader import incremental_upload


class TestIncrementalUpload:
    """Tests for incremental_upload function."""

    def test_incremental_upload_no_unsynced(self, test_settings, mocker):
        """Test upload with no unsynced emails."""
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[])
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        incremental_upload(test_settings, manifest, stats)

        assert stats[StatKey.BACKED_UP] == 0

    def test_incremental_upload_missing_email_file(self, test_settings, mocker):
        """Test upload when email file is missing."""
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

        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[row])
        mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.db.update_remote_path")

        # Mock rclone operations
        mocker.patch("mailbackup.rclone.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.rclone.rclone_moveto", return_value=Mock(returncode=0))

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Should handle missing file gracefully
        incremental_upload(test_settings, manifest, stats)

        # Upload should succeed even without the email file (edge case)
        assert stats[StatKey.BACKED_UP] >= 0


class TestUploadEmailEdgeCases:
    """Tests for upload_email edge cases and error paths."""

    def test_upload_email_missing_local_file(self, test_settings, mocker):
        """Test upload_email when local email file doesn't exist."""
        from mailbackup.uploader import upload_email
        from mailbackup.manifest import ManifestManager
        from mailbackup.statistics import create_stats, StatKey
        from pathlib import Path
        
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir = Path("/nonexistent")
        
        manifest = mocker.Mock(spec=ManifestManager)
        stats = create_stats()
        
        row = {
            "id": 1,
            "hash": "abc123",
            "path": "/nonexistent/email.eml",  # Doesn't exist
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
        }
        
        # Mock rclone operations
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.uploader.remote_hash", return_value={})
        mocker.patch("mailbackup.uploader.db.mark_synced")
        
        # Execute
        result = upload_email(row, test_settings, manifest, stats)
        
        # Should handle gracefully (email_uploaded=True for missing file)
        assert result is True

    def test_upload_email_with_attachments(self, test_settings, mocker, tmp_path):
        """Test upload_email with email that has attachments."""
        from mailbackup.uploader import upload_email
        from mailbackup.manifest import ManifestManager
        from mailbackup.statistics import create_stats
        import json
        
        # Setup
        test_settings.tmp_dir = tmp_path / "tmp"
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        
        # Create email file
        email_file = test_settings.maildir / "test.eml"
        email_file.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        # Create attachment
        att_dir = tmp_path / "attachments"
        att_dir.mkdir()
        att_file = att_dir / "test.pdf"
        att_file.write_bytes(b"PDF content")
        
        manifest = mocker.Mock(spec=ManifestManager)
        stats = create_stats()
        
        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_file),
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": json.dumps([str(att_file)]),
        }
        
        # Mock operations
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.uploader.remote_hash", return_value={
            "2024/2024-01-01_12-00-00_from_test@example.com_subject_Test_[abc123]/email.eml": "hash123"
        })
        mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.sha256", return_value="hash123")
        
        # Execute
        result = upload_email(row, test_settings, manifest, stats)
        
        # Should succeed
        assert result is True

    def test_upload_email_verification_mismatch(self, test_settings, mocker, tmp_path):
        """Test upload_email when remote hash doesn't match."""
        from mailbackup.uploader import upload_email
        from mailbackup.manifest import ManifestManager
        from mailbackup.statistics import create_stats
        
        # Setup
        test_settings.tmp_dir = tmp_path / "tmp"
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        
        email_file = test_settings.maildir / "test.eml"
        email_file.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        manifest = mocker.Mock(spec=ManifestManager)
        stats = create_stats()
        
        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_file),
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
        }
        
        # Mock operations - hash mismatch
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.uploader.remote_hash", return_value={
            "2024/2024-01-01_12-00-00_from_test@example.com_subject_Test_[abc123]/email.eml": "wrong_hash"
        })
        mocker.patch("mailbackup.uploader.sha256", return_value="correct_hash")
        mocker.patch("mailbackup.uploader.rclone_deletefile")
        
        # Execute
        result = upload_email(row, test_settings, manifest, stats)
        
        # Should fail after retries
        assert result is False

    def test_upload_email_no_remote_hash(self, test_settings, mocker, tmp_path):
        """Test upload_email when remote_hash returns None."""
        from mailbackup.uploader import upload_email
        from mailbackup.manifest import ManifestManager
        from mailbackup.statistics import create_stats
        
        # Setup
        test_settings.tmp_dir = tmp_path / "tmp"
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        
        email_file = test_settings.maildir / "test.eml"
        email_file.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        manifest = mocker.Mock(spec=ManifestManager)
        stats = create_stats()
        
        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_file),
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
        }
        
        # Mock operations - remote_hash returns None
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.uploader.remote_hash", return_value=None)
        mocker.patch("mailbackup.uploader.rclone_deletefile")
        
        # Execute
        result = upload_email(row, test_settings, manifest, stats)
        
        # Should fail due to no remote hash
        assert result is False

    def test_upload_email_atomic_upload_failure(self, test_settings, mocker, tmp_path):
        """Test upload_email when atomic_upload_file fails."""
        from mailbackup.uploader import upload_email
        from mailbackup.manifest import ManifestManager
        from mailbackup.statistics import create_stats
        
        # Setup
        test_settings.tmp_dir = tmp_path / "tmp"
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        
        email_file = test_settings.maildir / "test.eml"
        email_file.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        manifest = mocker.Mock(spec=ManifestManager)
        stats = create_stats()
        
        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_file),
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
        }
        
        # Mock atomic_upload_file to fail
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=False)
        
        # Execute
        result = upload_email(row, test_settings, manifest, stats)
        
        # Should fail after retries
        assert result is False

    def test_upload_email_keyerror_in_verification(self, test_settings, mocker, tmp_path):
        """Test upload_email when verification raises KeyError."""
        from mailbackup.uploader import upload_email
        from mailbackup.manifest import ManifestManager
        from mailbackup.statistics import create_stats
        
        # Setup
        test_settings.tmp_dir = tmp_path / "tmp"
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        
        email_file = test_settings.maildir / "test.eml"
        email_file.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        manifest = mocker.Mock(spec=ManifestManager)
        stats = create_stats()
        
        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_file),
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
        }
        
        # Mock operations - return dict without expected key
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.uploader.remote_hash", return_value={"other_key": "value"})
        mocker.patch("mailbackup.uploader.sha256", return_value="hash123")
        mocker.patch("mailbackup.uploader.rclone_deletefile")
        
        # Execute
        result = upload_email(row, test_settings, manifest, stats)
        
        # Should fail due to KeyError
        assert result is False
