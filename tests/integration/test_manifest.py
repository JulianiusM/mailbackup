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
