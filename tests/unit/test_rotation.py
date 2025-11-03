#!/usr/bin/env python3
"""
Unit tests for rotation.py module.
"""

import pytest
import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from mailbackup.rotation import rotate_archives
from mailbackup.config import Settings
from mailbackup.manifest import ManifestManager


class TestRotateArchives:
    """Tests for rotate_archives function."""
    
    @pytest.fixture
    def mock_rotation_deps(self, mocker):
        """Mock all rotation dependencies."""
        mocker.patch("mailbackup.rotation.rclone_lsf", return_value=Mock(returncode=1))
        mocker.patch("mailbackup.rotation.rclone_copy", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.rotation.run_cmd", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.rotation.sha256", return_value="archive_hash")
        mocker.patch("mailbackup.rotation.safe_write_json")
        mocker.patch("mailbackup.rotation.atomic_upload_file", return_value="archive_hash")
        mocker.patch("mailbackup.rotation.db.mark_archived_year")
        mocker.patch("shutil.rmtree")
        return mocker
    
    def test_rotate_archives_no_candidates(self, test_settings, mocker, mock_rotation_deps):
        """Test rotation with no candidate years."""
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=[])
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        rotate_archives(test_settings, manifest, stats)
        
        assert stats["archived"] == 0
    
    def test_rotate_archives_single_year(self, test_settings, mocker, mock_rotation_deps):
        """Test rotation with single year."""
        current_year = datetime.datetime.now(datetime.timezone.utc).year
        old_year = current_year - test_settings.retention_years - 1
        
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=[old_year])
        mocker.patch("mailbackup.rotation.db.fetch_unarchived_paths_for_year", return_value=["path1", "path2"])
        
        # Create archive file that run_cmd would create
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        archive_dir = test_settings.tmp_dir / "rotation" / str(old_year)
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_file = archive_dir / f"emails_{old_year}.tar.zst"
        archive_file.write_bytes(b"archive content")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        rotate_archives(test_settings, manifest, stats)
        
        assert stats["archived"] > 0
    
    def test_rotate_archives_multiple_years(self, test_settings, mocker, mock_rotation_deps):
        """Test rotation with multiple years."""
        current_year = datetime.datetime.now(datetime.timezone.utc).year
        old_years = [current_year - test_settings.retention_years - i for i in range(1, 4)]
        
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=old_years)
        mocker.patch("mailbackup.rotation.db.fetch_unarchived_paths_for_year", return_value=["path"])
        
        # Create archive files for all years
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        for year in old_years:
            archive_dir = test_settings.tmp_dir / "rotation" / str(year)
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_file = archive_dir / f"emails_{year}.tar.zst"
            archive_file.write_bytes(b"archive content")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        rotate_archives(test_settings, manifest, stats)
        
        # Should process all years
        assert stats["archived"] > 0
    
    def test_rotate_archives_skip_complete_year(self, test_settings, mocker, mock_rotation_deps):
        """Test rotation skips year when archive is complete."""
        current_year = datetime.datetime.now(datetime.timezone.utc).year
        old_year = current_year - test_settings.retention_years - 1
        
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=[old_year])
        mocker.patch("mailbackup.rotation.db.fetch_unarchived_paths_for_year", return_value=[])
        
        mocker.patch("mailbackup.rotation.rclone_lsf", return_value=Mock(returncode=0))  # Archive exists
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        rotate_archives(test_settings, manifest, stats)
        
        # Should skip year
        assert stats["archived"] == 0
    
    def test_rotate_archives_marks_archived(self, test_settings, mocker, mock_rotation_deps):
        """Test that rotation marks year as archived in DB."""
        current_year = datetime.datetime.now(datetime.timezone.utc).year
        old_year = current_year - test_settings.retention_years - 1
        
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=[old_year])
        mocker.patch("mailbackup.rotation.db.fetch_unarchived_paths_for_year", return_value=["path"])
        
        # Create archive file
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        archive_dir = test_settings.tmp_dir / "rotation" / str(old_year)
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_file = archive_dir / f"emails_{old_year}.tar.zst"
        archive_file.write_bytes(b"archive content")
        
        mock_mark = mocker.patch("mailbackup.rotation.db.mark_archived_year")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        rotate_archives(test_settings, manifest, stats)
        
        mock_mark.assert_called()
    
    def test_rotate_archives_queues_manifest(self, test_settings, mocker, mock_rotation_deps):
        """Test that rotation queues archive in manifest."""
        current_year = datetime.datetime.now(datetime.timezone.utc).year
        old_year = current_year - test_settings.retention_years - 1
        
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=[old_year])
        mocker.patch("mailbackup.rotation.db.fetch_unarchived_paths_for_year", return_value=["path"])
        
        # Create archive file
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        archive_dir = test_settings.tmp_dir / "rotation" / str(old_year)
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_file = archive_dir / f"emails_{old_year}.tar.zst"
        archive_file.write_bytes(b"archive content")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        rotate_archives(test_settings, manifest, stats)
        
        manifest.queue_entry.assert_called()
    
    def test_rotate_archives_handles_error_gracefully(self, test_settings, mocker, mock_rotation_deps):
        """Test that rotation handles errors gracefully and continues."""
        current_year = datetime.datetime.now(datetime.timezone.utc).year
        old_years = [current_year - test_settings.retention_years - i for i in range(1, 3)]
        
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=old_years)
        mocker.patch("mailbackup.rotation.db.fetch_unarchived_paths_for_year", return_value=["path"])
        
        # First year fails, second succeeds
        call_count = [0]
        def side_effect_lsf(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Error")
            return Mock(returncode=1)
        
        mocker.patch("mailbackup.rotation.rclone_lsf", side_effect=side_effect_lsf)
        
        # Create archive file for second year
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        archive_dir = test_settings.tmp_dir / "rotation" / str(old_years[1])
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_file = archive_dir / f"emails_{old_years[1]}.tar.zst"
        archive_file.write_bytes(b"archive content")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        # Should not raise exception
        rotate_archives(test_settings, manifest, stats)
    
    def test_rotate_archives_with_existing_archive(self, test_settings, mocker, mock_rotation_deps):
        """Test rotation merges with existing archive."""
        current_year = datetime.datetime.now(datetime.timezone.utc).year
        old_year = current_year - test_settings.retention_years - 1
        
        mocker.patch("mailbackup.rotation.db.get_candidate_rotation_years", return_value=[old_year])
        mocker.patch("mailbackup.rotation.db.fetch_unarchived_paths_for_year", return_value=["path"])
        
        # Mock that archive exists  
        mocker.patch("mailbackup.rotation.rclone_lsf", return_value=Mock(returncode=0))
        mock_copy = mocker.patch("mailbackup.rotation.rclone_copy", return_value=Mock(returncode=0))
        
        # Create archive file
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        archive_dir = test_settings.tmp_dir / "rotation" / str(old_year)
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_file = archive_dir / f"emails_{old_year}.tar.zst"
        archive_file.write_bytes(b"archive content")
        
        manifest = Mock(spec=ManifestManager)
        stats = {"archived": 0}
        
        # Archive download should be attempted
        rotate_archives(test_settings, manifest, stats)
        
        # rclone_copy should be called to download existing archive
        assert mock_copy.call_count > 0
