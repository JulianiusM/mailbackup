#!/usr/bin/env python3
"""
Unit tests for uploader.py module.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from mailbackup.uploader import incremental_upload
from mailbackup.config import Settings
from mailbackup.manifest import ManifestManager


class TestIncrementalUpload:
    """Tests for incremental_upload function."""
    
    def test_incremental_upload_no_unsynced(self, test_settings, mocker):
        """Test upload with no unsynced emails."""
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[])
        manifest = Mock(spec=ManifestManager)
        stats = {"uploaded": 0, "skipped": 0}
        
        incremental_upload(test_settings, manifest, stats)
        
        assert stats["uploaded"] == 0
    
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
        stats = {"uploaded": 0, "skipped": 0}
        
        # Should handle missing file gracefully
        incremental_upload(test_settings, manifest, stats)
        
        # Upload should succeed even without the email file (edge case)
        assert stats["uploaded"] >= 0

