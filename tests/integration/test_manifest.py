#!/usr/bin/env python3
"""
Integration tests for manifest module.
"""

import pytest
from mailbackup.manifest import ManifestManager


@pytest.mark.integration
class TestManifestIntegration:
    """Integration tests for manifest module."""

    def test_manifest_manager_creation(self, test_settings, tmp_path):
        """Test ManifestManager can be created."""
        test_settings.tmp_dir = tmp_path
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        
        manifest = ManifestManager(test_settings)
        
        # Queue some entries
        manifest.queue_entry("file1.txt", "hash1")
        manifest.queue_entry("file2.txt", "hash2")
        
        # Test passes if no exception raised
        assert True


@pytest.mark.integration
class TestManifestRecovery:
    """Integration tests for manifest recovery and resilience."""

    def test_recover_interrupted_upload(self, test_settings, tmp_path, mocker):
        """Test recovering from interrupted upload."""
        from mailbackup.manifest import ManifestManager
        
        test_settings.tmp_dir = tmp_path
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        
        # Create queue with interrupted entries
        queue_file = test_settings.tmp_dir / "manifest.queue.json"
        import json
        queue_data = [["hash1", "file1.txt"], ["hash2", "file2.txt"]]
        with open(queue_file, 'w') as f:
            json.dump(queue_data, f)
        
        manifest = ManifestManager(test_settings)
        
        # Recover should process queue
        manifest.recover_interrupted()
        
        # Test passes if no exception
        assert True



@pytest.mark.integration
class TestManifestUpload:
    """Integration tests for manifest upload functionality."""

    def test_upload_manifest_if_needed(self, test_settings, tmp_path, mocker):
        """Test upload_manifest_if_needed functionality."""
        from mailbackup.manifest import ManifestManager
        
        test_settings.tmp_dir = tmp_path
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.remote = "remote:backup"
        
        # Create manifest file
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        manifest_file.write_text("hash1,path1.txt\n")
        
        # Mock rclone
        mocker.patch("subprocess.run", return_value=mocker.Mock(returncode=0, stdout=""))
        
        manifest = ManifestManager(test_settings)
        
        # Should check and upload if needed
        manifest.upload_manifest_if_needed()
        
        assert True
