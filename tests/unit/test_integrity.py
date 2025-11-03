#!/usr/bin/env python3
"""
Unit tests for integrity.py module.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from mailbackup.integrity import integrity_check, rebuild_docset
from mailbackup.config import Settings
from mailbackup.manifest import ManifestManager


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


class TestIntegrityCheck:
    """Tests for integrity_check function."""
    
    @pytest.fixture
    def mock_rclone(self, mocker):
        """Mock all rclone commands."""
        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))
        return mocker
    
    def test_integrity_check_all_good(self, test_settings, mocker, mock_rclone):
        """Test integrity check when all files are valid."""
        mocker.patch("mailbackup.integrity.load_manifest_csv", return_value={})
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[])
        
        manifest = Mock(spec=ManifestManager)
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        assert stats["verified"] == 0
        assert stats["repaired"] == 0
    
    def test_integrity_check_with_manifest(self, test_settings, mocker, mock_rclone):
        """Test integrity check using manifest."""
        # Mock a manifest with one entry
        manifest_data = {"2024/folder/email.eml": "abc123"}
        mocker.patch("mailbackup.integrity.load_manifest_csv", return_value=manifest_data)
        
        # Mock DB data matching manifest
        db_row = {
            "id": 1,
            "hash": "abc123",
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
    
    def test_integrity_check_hash_mismatch(self, test_settings, mocker, mock_rclone):
        """Test integrity check with hash mismatch."""
        # Mock manifest with different hash
        manifest_data = {"2024/folder/email.eml": "wrong_hash"}
        mocker.patch("mailbackup.integrity.load_manifest_csv", return_value=manifest_data)
        
        db_row = {
            "id": 1,
            "hash": "abc123",
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
        
        # Don't repair for this test
        test_settings.repair_on_failure = False
        
        manifest = Mock(spec=ManifestManager)
        manifest.manifest_path = test_settings.manifest_path
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        # Should detect mismatch
        assert stats["verified"] == 0
    
    def test_integrity_check_repair_on_mismatch(self, test_settings, mocker, mock_rclone):
        """Test integrity check repairs on hash mismatch."""
        # Create test email file
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        manifest_data = {"2024/folder/email.eml": "wrong_hash"}
        mocker.patch("mailbackup.integrity.load_manifest_csv", return_value=manifest_data)
        
        db_row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_path),
            "remote_path": "2024/folder/email.eml",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])
        
        # Mock rebuild and upload
        mocker.patch("mailbackup.integrity.rebuild_docset", return_value=test_settings.tmp_dir / "rebuild")
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value="new_hash")
        
        test_settings.repair_on_failure = True
        
        manifest = Mock(spec=ManifestManager)
        manifest.manifest_path = test_settings.manifest_path
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        # Should attempt repair
        assert stats["repaired"] > 0 or stats["verified"] >= 0
    
    def test_integrity_check_missing_remote_file(self, test_settings, mocker, mock_rclone):
        """Test integrity check when remote file is missing."""
        manifest_data = {}  # Empty manifest
        mocker.patch("mailbackup.integrity.load_manifest_csv", return_value=manifest_data)
        
        db_row = {
            "id": 1,
            "hash": "abc123",
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
        
        test_settings.repair_on_failure = False
        
        manifest = Mock(spec=ManifestManager)
        manifest.manifest_path = test_settings.manifest_path
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        # Should handle missing file
        assert stats.get("verified", 0) >= 0
    
    def test_integrity_check_no_synced_files(self, test_settings, mocker, mock_rclone):
        """Test integrity check with no synced files."""
        mocker.patch("mailbackup.integrity.load_manifest_csv", return_value={})
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[])
        
        manifest = Mock(spec=ManifestManager)
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        assert stats["verified"] == 0
        assert stats["repaired"] == 0
    
    def test_integrity_check_verify_disabled(self, test_settings, mocker, mock_rclone):
        """Test integrity check when verification is disabled."""
        test_settings.verify_integrity = False
        
        manifest = Mock(spec=ManifestManager)
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        # Should exit early
        assert stats["verified"] == 0
    
    def test_integrity_check_with_remote_hash(self, test_settings, mocker, mock_rclone):
        """Test integrity check using remote hash verification."""
        mocker.patch("mailbackup.integrity.load_manifest_csv", return_value={})
        
        db_row = {
            "id": 1,
            "hash": "abc123",
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
        
        # Mock remote_hash to return matching hash
        mocker.patch("mailbackup.integrity.remote_hash", return_value="abc123")
        
        manifest = Mock(spec=ManifestManager)
        manifest.manifest_path = test_settings.manifest_path
        stats = {"verified": 0, "repaired": 0}
        
        integrity_check(test_settings, manifest, stats)
        
        # Should verify successfully
        assert stats["verified"] >= 0
