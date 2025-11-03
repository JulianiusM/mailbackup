#!/usr/bin/env python3
"""
Unit tests for manifest.py module.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from mailbackup.manifest import (
    load_manifest_csv,
    _manifest_dict_to_lines,
    ManifestManager,
)
from mailbackup.config import Settings


class TestLoadManifestCsv:
    """Tests for load_manifest_csv function."""
    
    def test_load_manifest_csv_empty_file(self, tmp_path):
        """Test loading an empty manifest file."""
        manifest_path = tmp_path / "manifest.csv"
        manifest_path.write_text("")
        
        result = load_manifest_csv(manifest_path)
        assert result == {}
    
    def test_load_manifest_csv_nonexistent_file(self, tmp_path):
        """Test loading a non-existent manifest file."""
        manifest_path = tmp_path / "nonexistent.csv"
        
        result = load_manifest_csv(manifest_path)
        assert result == {}
    
    def test_load_manifest_csv_with_entries(self, tmp_path):
        """Test loading manifest with entries."""
        manifest_path = tmp_path / "manifest.csv"
        manifest_path.write_text("abc123,path/to/file1.eml\ndef456,path/to/file2.eml\n")
        
        result = load_manifest_csv(manifest_path)
        assert result == {
            "path/to/file1.eml": "abc123",
            "path/to/file2.eml": "def456",
        }
    
    def test_load_manifest_csv_skips_invalid_lines(self, tmp_path):
        """Test that invalid lines are skipped."""
        manifest_path = tmp_path / "manifest.csv"
        manifest_path.write_text("abc123,path/to/file1.eml\ninvalid_line\n\ndef456,path/to/file2.eml\n")
        
        result = load_manifest_csv(manifest_path)
        assert len(result) == 2
        assert "path/to/file1.eml" in result
        assert "path/to/file2.eml" in result
    
    def test_load_manifest_csv_strips_whitespace(self, tmp_path):
        """Test that whitespace is stripped from entries."""
        manifest_path = tmp_path / "manifest.csv"
        manifest_path.write_text("  abc123  ,  path/to/file.eml  \n")
        
        result = load_manifest_csv(manifest_path)
        assert result == {"path/to/file.eml": "abc123"}


class TestManifestDictToLines:
    """Tests for _manifest_dict_to_lines function."""
    
    def test_manifest_dict_to_lines_empty(self):
        """Test with empty dict."""
        result = _manifest_dict_to_lines({})
        assert result == []
    
    def test_manifest_dict_to_lines_single_entry(self):
        """Test with single entry."""
        result = _manifest_dict_to_lines({"path/to/file.eml": "abc123"})
        assert result == ["abc123,path/to/file.eml\n"]
    
    def test_manifest_dict_to_lines_multiple_entries(self):
        """Test with multiple entries."""
        result = _manifest_dict_to_lines({
            "b_path.eml": "hash2",
            "a_path.eml": "hash1",
            "c_path.eml": "hash3",
        })
        # Should be sorted by key
        assert result == [
            "hash1,a_path.eml\n",
            "hash2,b_path.eml\n",
            "hash3,c_path.eml\n",
        ]


class TestManifestManager:
    """Tests for ManifestManager class."""
    
    def test_init(self, test_settings):
        """Test ManifestManager initialization."""
        manager = ManifestManager(test_settings)
        
        assert manager.settings == test_settings
        assert manager.manifest_path == test_settings.manifest_path
        assert manager.tmp_dir == test_settings.tmp_dir
        assert manager.remote == test_settings.remote
        assert manager._manifest_queue == {}
    
    def test_queue_entry(self, test_settings, mocker):
        """Test queueing a manifest entry."""
        mocker.patch("mailbackup.manifest.write_json_atomic")
        manager = ManifestManager(test_settings)
        
        manager.queue_entry("path/to/file.eml", "abc123")
        
        assert manager._manifest_queue == {"path/to/file.eml": "abc123"}
    
    def test_queue_entry_multiple(self, test_settings, mocker):
        """Test queueing multiple manifest entries."""
        mocker.patch("mailbackup.manifest.write_json_atomic")
        manager = ManifestManager(test_settings)
        
        manager.queue_entry("path1.eml", "hash1")
        manager.queue_entry("path2.eml", "hash2")
        
        assert manager._manifest_queue == {
            "path1.eml": "hash1",
            "path2.eml": "hash2",
        }
    
    def test_queue_entry_persists_immediately(self, test_settings, mocker):
        """Test that queue entry persists immediately."""
        mock_write = mocker.patch("mailbackup.manifest.write_json_atomic")
        manager = ManifestManager(test_settings)
        
        manager.queue_entry("path.eml", "hash")
        
        mock_write.assert_called_once()
    
    def test_dump_queue_empty(self, test_settings, mocker):
        """Test dumping empty queue."""
        mock_write = mocker.patch("mailbackup.manifest.write_json_atomic")
        manager = ManifestManager(test_settings)
        
        manager.dump_queue()
        
        # Should not write if queue is empty
        mock_write.assert_not_called()
    
    def test_dump_queue_with_entries(self, test_settings, mocker):
        """Test dumping queue with entries."""
        mock_write = mocker.patch("mailbackup.manifest.write_json_atomic")
        manager = ManifestManager(test_settings)
        manager._manifest_queue = {"path.eml": "hash"}
        
        manager.dump_queue()
        
        mock_write.assert_called()
    
    def test_dump_queue_handles_exception(self, test_settings, mocker):
        """Test that dump_queue handles exceptions gracefully."""
        mock_write = mocker.patch("mailbackup.manifest.write_json_atomic")
        mock_write.side_effect = Exception("Write error")
        manager = ManifestManager(test_settings)
        manager._manifest_queue = {"path.eml": "hash"}
        
        # Should not raise exception
        manager.dump_queue()
    
    def test_restore_queue_no_file(self, test_settings):
        """Test restoring queue when no dump file exists."""
        manager = ManifestManager(test_settings)
        
        manager.restore_queue()
        
        assert manager._manifest_queue == {}
    
    def test_restore_queue_with_file(self, test_settings):
        """Test restoring queue from dump file."""
        queue_data = {"path1.eml": "hash1", "path2.eml": "hash2"}
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        queue_file = test_settings.tmp_dir / "manifest.queue.json"
        queue_file.write_text(json.dumps(queue_data))
        
        manager = ManifestManager(test_settings)
        manager.restore_queue()
        
        assert manager._manifest_queue == queue_data
    
    def test_restore_queue_invalid_json(self, test_settings):
        """Test restoring queue with invalid JSON."""
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        queue_file = test_settings.tmp_dir / "manifest.queue.json"
        queue_file.write_text("invalid json")
        
        manager = ManifestManager(test_settings)
        manager.restore_queue()
        
        # Should handle error gracefully
        assert manager._manifest_queue == {}
    
    def test_recover_interrupted_no_file(self, test_settings):
        """Test recovering when no interrupted upload exists."""
        manager = ManifestManager(test_settings)
        
        # Should not raise exception
        manager.recover_interrupted()
    
    def test_recover_interrupted_with_file(self, test_settings, mocker):
        """Test recovering interrupted upload."""
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        inprogress_file = test_settings.tmp_dir / "manifest.uploading"
        inprogress_file.write_text("hash1,path1.eml\nhash2,path2.eml\n")
        
        mock_rclone_copyto = mocker.patch("mailbackup.manifest.rclone_copyto")
        mock_rclone_copyto.return_value = Mock(returncode=0)
        mock_rclone_lsjson = mocker.patch("mailbackup.manifest.rclone_lsjson")
        mock_rclone_lsjson.return_value = Mock(returncode=1, stdout="[]")
        mock_rclone_moveto = mocker.patch("mailbackup.manifest.rclone_moveto")
        mock_rclone_moveto.return_value = Mock(returncode=0)
        
        manager = ManifestManager(test_settings)
        manager.recover_interrupted()
        
        # File should be removed after recovery
        assert not inprogress_file.exists()
    
    def test_upload_manifest_if_needed_empty_queue(self, test_settings, mocker):
        """Test uploading manifest when queue is empty."""
        manager = ManifestManager(test_settings)
        
        # With empty queue, should not attempt upload
        manager.upload_manifest_if_needed()
        
        # No assertion needed - just verify no exception
    
    def test_thread_safety(self, test_settings, mocker):
        """Test that operations are thread-safe."""
        import threading
        mocker.patch("mailbackup.manifest.write_json_atomic")
        
        manager = ManifestManager(test_settings)
        
        def add_entries():
            for i in range(10):
                manager.queue_entry(f"path{i}.eml", f"hash{i}")
        
        threads = [threading.Thread(target=add_entries) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should have 10 entries (last write wins for same key)
        assert len(manager._manifest_queue) == 10
