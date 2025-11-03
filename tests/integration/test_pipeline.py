#!/usr/bin/env python3
"""
Integration tests for uploader, manifest, and integrity modules.
Tests the actual integration with mocked rclone commands.
"""

import pytest
import json
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from mailbackup.uploader import incremental_upload
from mailbackup.manifest import ManifestManager
from mailbackup.integrity import integrity_check


@pytest.mark.integration
class TestUploaderIntegration:
    """Integration tests for uploader module."""
    
    def test_incremental_upload_single_email_integration(self, test_settings, mocker, tmp_path):
        """Test complete upload workflow with all dependencies."""
        # Create real test email file
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        # Mock database
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
        mock_update_path = mocker.patch("mailbackup.uploader.db.update_remote_path")
        
        # Mock rclone calls
        mocker.patch("mailbackup.rclone.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.rclone.rclone_moveto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.rclone.rclone_deletefile", return_value=Mock(returncode=0))
        
        # Mock remote hash verification
        def mock_remote_hash(settings, path):
            # Return matching hash
            from mailbackup.utils import sha256
            expected_hash = sha256(email_path)
            return {path: expected_hash}
        mocker.patch("mailbackup.uploader.remote_hash", side_effect=mock_remote_hash)
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["uploaded"] == 1
        mock_mark_synced.assert_called_once()
        manifest.queue_entry.assert_called()
    
    def test_incremental_upload_with_attachments_integration(self, test_settings, mocker, tmp_path):
        """Test upload with attachments."""
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
            "attachments": json.dumps([str(attach_path)]),
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[row])
        mocker.patch("mailbackup.uploader.db.mark_synced")
        mocker.patch("mailbackup.uploader.db.update_remote_path")
        
        # Mock rclone
        mocker.patch("mailbackup.rclone.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.rclone.rclone_moveto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.rclone.rclone_deletefile", return_value=Mock(returncode=0))
        
        def mock_remote_hash(settings, path):
            from mailbackup.utils import sha256
            expected_hash = sha256(email_path)
            return {path: expected_hash}
        mocker.patch("mailbackup.uploader.remote_hash", side_effect=mock_remote_hash)
        
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["uploaded"] == 1


@pytest.mark.integration
class TestManifestIntegration:
    """Integration tests for manifest module."""
    
    def test_upload_manifest_resilient_integration(self, test_settings, mocker):
        """Test manifest upload with mocked rclone."""
        # Mock rclone operations
        mocker.patch("mailbackup.manifest.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.manifest.rclone_lsjson", return_value=Mock(returncode=1, stdout="[]"))
        mocker.patch("mailbackup.manifest.rclone_moveto", return_value=Mock(returncode=0))
        
        manager = ManifestManager(test_settings)
        manager._manifest_queue = {"path.eml": "hash"}
        
        # Should not raise exception
        manager.upload_manifest_resilient()


@pytest.mark.integration  
class TestIntegrityIntegration:
    """Integration tests for integrity checking."""
    
    def test_integrity_check_with_manifest_integration(self, test_settings, mocker):
        """Test integrity check using manifest file."""
        # Create a manifest file
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        manifest_file.write_text("abc123,2024/folder/email.eml\n")
        
        # Mock rclone copyto
        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))
        
        # Mock DB data matching manifest
        db_row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "abc123",
            "path": "/path/to/email.eml",
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
        manifest.manifest_path = test_settings.manifest_path
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        assert stats["verified"] == 1
    
    def test_integrity_check_no_manifest_integration(self, test_settings, mocker):
        """Test integrity check when manifest doesn't exist."""
        # Mock rclone copyto to not create file
        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))
        
        # Mock remote_hash to return None (no hashsum support)
        mocker.patch("mailbackup.integrity.remote_hash", return_value=None)
        
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[])
        
        manifest = Mock(spec=ManifestManager)
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        # Should exit early when no remote hash available
        assert stats["verified"] == 0
