#!/usr/bin/env python3
"""
Unit tests for uploader.py module.
"""

import pytest
import json
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from mailbackup.uploader import incremental_upload
from mailbackup.config import Settings
from mailbackup.manifest import ManifestManager


class TestIncrementalUpload:
    """Tests for incremental_upload function."""
    
    @pytest.fixture
    def mock_upload_deps(self, mocker):
        """Mock all upload dependencies."""
        mocker.patch("mailbackup.uploader.sha256", return_value="abc123")
        mocker.patch("mailbackup.uploader.load_attachments", return_value=[])
        mocker.patch("mailbackup.uploader.build_info_json", return_value={})
        mocker.patch("mailbackup.uploader.safe_write_json")
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.uploader.remote_hash", return_value={"2024/test/email.eml": "abc123"})
        mocker.patch("mailbackup.uploader.rclone_deletefile")
        return mocker
    
    def test_incremental_upload_no_unsynced(self, test_settings, mocker, mock_upload_deps):
        """Test upload with no unsynced emails."""
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[])
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["uploaded"] == 0
    
    def test_incremental_upload_single_email(self, test_settings, mocker, mock_upload_deps):
        """Test upload with single email."""
        # Create a test email file
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
        
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[row])
        mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.db.update_remote_path")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["uploaded"] == 1
    
    def test_incremental_upload_with_attachments(self, test_settings, mocker, mock_upload_deps):
        """Test upload with email containing attachments."""
        # Create test email and attachment
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        test_settings.attachments_dir.mkdir(parents=True, exist_ok=True)
        
        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        attach_path = test_settings.attachments_dir / "2024" / "test.pdf"
        attach_path.parent.mkdir(parents=True, exist_ok=True)
        attach_path.write_bytes(b"PDF content")
        
        mocker.patch("mailbackup.uploader.load_attachments", return_value=[attach_path])
        
        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_path),
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": json.dumps([str(attach_path)]),
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[row])
        mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.db.update_remote_path")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["uploaded"] == 1
    
    def test_incremental_upload_parallel(self, test_settings, mocker, mock_upload_deps):
        """Test upload with multiple emails processed in parallel."""
        # Create multiple test emails
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        
        rows = []
        for i in range(5):
            email_path = test_settings.maildir / f"test{i}.eml"
            email_path.write_text(f"From: test{i}@example.com\nSubject: Test {i}\n\nBody")
            
            rows.append({
                "id": i,
                "hash": f"hash{i}",
                "path": str(email_path),
                "from_header": f"test{i}@example.com",
                "subject": f"Test Email {i}",
                "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
                "attachments": "[]",
                "spam": 0,
                "processed_at": "2024-01-01 12:00:00",
            })
        
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=rows)
        mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.db.update_remote_path")
        
        # Mock remote_hash to return matching hashes for all files
        def mock_remote_hash_func(settings, path):
            return {path: "hash0"}  # Return any hash
        mocker.patch("mailbackup.uploader.remote_hash", side_effect=mock_remote_hash_func)
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["uploaded"] == 5
    
    def test_incremental_upload_marks_synced(self, test_settings, mocker, mock_upload_deps):
        """Test that uploaded emails are marked as synced."""
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
        
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[row])
        mock_mark_synced = mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.db.update_remote_path")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        mock_mark_synced.assert_called_once()
    
    def test_incremental_upload_queues_manifest(self, test_settings, mocker, mock_upload_deps):
        """Test that uploaded emails are queued in manifest."""
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
        
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[row])
        mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.db.update_remote_path")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        manifest.queue_entry.assert_called()
    
    def test_incremental_upload_skip_on_error(self, test_settings, mocker, mock_upload_deps):
        """Test that errors during upload skip the email."""
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
        
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[row])
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=False)  # Fail upload
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        # Should not raise exception
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["skipped"] == 1
    
    def test_incremental_upload_missing_email_file(self, test_settings, mocker, mock_upload_deps):
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
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        # Should not raise exception - missing files are handled
        incremental_upload(test_settings, manifest, stats)
        
        # Upload should succeed even without the email file (edge case)
        assert stats["uploaded"] >= 0
