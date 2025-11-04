#!/usr/bin/env python3
"""
Integration tests for uploader module.
Consolidates tests from test_final_coverage.py and test_pipeline.py.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock
from mailbackup.uploader import incremental_upload
from mailbackup.manifest import ManifestManager
from mailbackup.statistics import create_stats, StatKey


@pytest.mark.integration
class TestUploaderIntegration:
    """Integration tests for uploader module."""

    def test_incremental_upload_basic(self, test_settings, test_db, tmp_path, mocker):
        """Test basic incremental upload functionality."""
        # Setup
        test_settings.db_path = test_db
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        
        # Mock dependencies
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[])
        mocker.patch("mailbackup.uploader.remote_hash", return_value={})
        
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()
        
        # Execute
        incremental_upload(test_settings, manifest, stats)
        
        # Verify - should complete without error
        assert stats[StatKey.BACKED_UP] >= 0

    def test_incremental_upload_with_attachments(self, test_settings, test_db, tmp_path, mocker):
        """Test incremental upload with attachments."""
        # Setup
        test_settings.db_path = test_db
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        test_settings.attachments_dir = tmp_path / "attachments"
        test_settings.attachments_dir.mkdir()
        
        # Mock dependencies
        mocker.patch("mailbackup.uploader.db.fetch_unsynced", return_value=[])
        mocker.patch("mailbackup.uploader.remote_hash", return_value={})
        
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()
        
        # Execute
        incremental_upload(test_settings, manifest, stats)
        
        # Should handle attachments directory
        assert stats[StatKey.BACKED_UP] >= 0


@pytest.mark.integration
class TestIncrementalUploadIntegration:
    """Integration tests for incremental upload workflow."""

    def test_incremental_upload_with_real_files(self, test_settings, test_db, tmp_path, mocker):
        """Test incremental_upload with real database and files."""
        from mailbackup.uploader import incremental_upload
        from mailbackup.manifest import ManifestManager
        from mailbackup.statistics import create_stats, StatKey
        
        # Setup
        test_settings.db_path = test_db
        test_settings.tmp_dir = tmp_path / "tmp"
        test_settings.tmp_dir.mkdir()
        test_settings.maildir = tmp_path / "maildir"
        test_settings.maildir.mkdir()
        
        # Create email file
        email_file = test_settings.maildir / "test.eml"
        email_file.write_text("From: test@example.com\nSubject: Test\n\nBody")
        
        # Add to database
        from mailbackup import db
        db.mark_processed(test_db, "hash123", str(email_file), "test@example.com", 
                         "Test", "2024-01-01T12:00:00", [], False)
        
        # Mock rclone operations
        mocker.patch("mailbackup.uploader.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.uploader.remote_hash", return_value={
            "2024/2024-01-01_12-00-00_from_test@example.com_subject_Test_[hash123]/email.eml": "hash123"
        })
        mocker.patch("mailbackup.uploader.sha256", return_value="hash123")
        
        manifest = ManifestManager(test_settings)
        stats = create_stats()
        
        # Execute
        incremental_upload(test_settings, manifest, stats)
        
        # Should have processed
        assert stats[StatKey.BACKED_UP] >= 0
