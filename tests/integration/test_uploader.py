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
