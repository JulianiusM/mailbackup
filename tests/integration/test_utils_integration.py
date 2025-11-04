#!/usr/bin/env python3
"""
Integration tests for utils module.
These tests cover the integration paths not covered by unit tests.
"""

import json
import signal
import subprocess
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from mailbackup.statistics import StatusThread, create_stats, StatKey
from mailbackup.utils import (
    run_streaming,
    atomic_upload_file,
    compute_remote_sha256,
    remote_hash,
    working_dir,
    ensure_dirs,
    install_signal_handlers,
)


@pytest.mark.integration
class TestRunStreamingIntegration:
    """Integration tests for run_streaming with real subprocess."""

    def test_run_streaming_success(self):
        """Test run_streaming with a successful command."""
        result = run_streaming("echo test", ["echo", "hello"], ignore_errors=True)
        assert result is True

    def test_run_streaming_failure_ignored(self):
        """Test run_streaming with failing command when errors are ignored."""
        result = run_streaming("false command", ["false"], ignore_errors=True)
        assert result is False

    def test_run_streaming_failure_raises(self):
        """Test run_streaming raises on failure when ignore_errors=False."""
        with pytest.raises(subprocess.CalledProcessError):
            run_streaming("false command", ["false"], ignore_errors=False)

    def test_run_streaming_with_output(self, capsys):
        """Test run_streaming logs output from command."""
        result = run_streaming("multiline", ["printf", "line1\\nline2\\nline3"], ignore_errors=True)
        assert result is True


@pytest.mark.integration
class TestAtomicUploadIntegration:
    """Integration tests for atomic_upload_file."""

    def test_atomic_upload_file_success(self, tmp_path, mocker):
        """Test successful atomic upload."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("test content")

        # Mock rclone operations
        mocker.patch("mailbackup.utils.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.utils.rclone_moveto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.utils.rclone_deletefile", return_value=Mock(returncode=0))

        result = atomic_upload_file(local_file, "remote:path/file.txt")
        assert result is True

    def test_atomic_upload_file_copyto_fails(self, tmp_path, mocker):
        """Test atomic upload when copyto fails."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("test content")

        # Mock rclone copyto to fail
        mocker.patch("mailbackup.utils.rclone_copyto", return_value=Mock(returncode=1))
        mock_delete = mocker.patch("mailbackup.utils.rclone_deletefile", return_value=Mock(returncode=0))

        result = atomic_upload_file(local_file, "remote:path/file.txt")
        assert result is False
        # Should attempt cleanup
        mock_delete.assert_called_once()

    def test_atomic_upload_file_moveto_fails(self, tmp_path, mocker):
        """Test atomic upload when moveto fails."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("test content")

        # Mock rclone operations
        mocker.patch("mailbackup.utils.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.utils.rclone_moveto", return_value=Mock(returncode=1))
        mock_delete = mocker.patch("mailbackup.utils.rclone_deletefile", return_value=Mock(returncode=0))

        result = atomic_upload_file(local_file, "remote:path/file.txt")
        assert result is False
        # Should attempt cleanup
        mock_delete.assert_called_once()


@pytest.mark.integration
class TestComputeRemoteHashIntegration:
    """Integration tests for compute_remote_sha256."""

    def test_compute_remote_sha256_success(self, test_settings, mocker):
        """Test computing SHA256 from remote file."""
        # Mock rclone_cat to return some content
        mocker.patch("mailbackup.utils.rclone_cat", return_value=Mock(
            returncode=0,
            stdout="test content"
        ))

        result = compute_remote_sha256(test_settings, "path/to/file.txt")
        assert result != ""
        assert len(result) == 64  # SHA256 hex digest length

    def test_compute_remote_sha256_failure(self, test_settings, mocker):
        """Test compute_remote_sha256 when rclone_cat fails."""
        # Mock rclone_cat to raise exception
        mocker.patch("mailbackup.utils.rclone_cat", side_effect=Exception("Failed"))

        result = compute_remote_sha256(test_settings, "path/to/file.txt")
        assert result == ""

    def test_compute_remote_sha256_bytes_output(self, test_settings, mocker):
        """Test compute_remote_sha256 with bytes output."""
        # Mock rclone_cat to return bytes
        mocker.patch("mailbackup.utils.rclone_cat", return_value=Mock(
            returncode=0,
            stdout=b"test content bytes"
        ))

        result = compute_remote_sha256(test_settings, "path/to/file.txt")
        assert result != ""
        assert len(result) == 64


@pytest.mark.integration
class TestRemoteHashIntegration:
    """Integration tests for remote_hash function."""

    def test_remote_hash_with_hashsum_support(self, test_settings, mocker):
        """Test remote_hash when hashsum is supported."""
        # Mock successful hashsum
        hashsum_output = "abc123  path/to/file1.eml\ndef456  path/to/file2.eml\n"
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(
            returncode=0,
            stdout=hashsum_output
        ))

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is not None
        assert len(result) == 2
        assert result["path/to/file1.eml"] == "abc123"
        assert result["path/to/file2.eml"] == "def456"

    def test_remote_hash_without_hashsum_support(self, test_settings, mocker):
        """Test remote_hash fallback when hashsum not supported."""
        # Mock failed hashsum
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(
            returncode=1,
            stdout=""
        ))

        # Mock lsjson to return file list
        lsjson_output = json.dumps([
            {"Path": "file1.eml"},
            {"Path": "file2.eml"},
        ])
        mocker.patch("mailbackup.utils.rclone_lsjson", return_value=Mock(
            returncode=0,
            stdout=lsjson_output
        ))

        # Mock compute_remote_sha256
        def mock_compute(settings, path):
            return f"hash_{path}"

        mocker.patch("mailbackup.utils.compute_remote_sha256", side_effect=mock_compute)

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is not None
        assert len(result) == 2
        assert result["file1.eml"] == "hash_file1.eml"
        assert result["file2.eml"] == "hash_file2.eml"

    def test_remote_hash_lsjson_fails(self, test_settings, mocker):
        """Test remote_hash when lsjson fails."""
        # Mock failed hashsum
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(
            returncode=1,
            stdout=""
        ))

        # Mock failed lsjson
        mocker.patch("mailbackup.utils.rclone_lsjson", return_value=Mock(
            returncode=1,
            stdout=""
        ))

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is None

    def test_remote_hash_empty_hashsum(self, test_settings, mocker):
        """Test remote_hash when hashsum returns no results."""
        # Mock successful hashsum but empty results
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(
            returncode=0,
            stdout=""
        ))

        # Should fallback to lsjson
        lsjson_output = json.dumps([{"Path": "file1.eml"}])
        mocker.patch("mailbackup.utils.rclone_lsjson", return_value=Mock(
            returncode=0,
            stdout=lsjson_output
        ))

        mocker.patch("mailbackup.utils.compute_remote_sha256", return_value="hash123")

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is not None
        assert len(result) == 1

    def test_remote_hash_compute_failure(self, test_settings, mocker):
        """Test remote_hash when compute_remote_sha256 raises exception."""
        # Mock failed hashsum
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(
            returncode=1,
            stdout=""
        ))

        # Mock lsjson
        lsjson_output = json.dumps([{"Path": "file1.eml"}, {"Path": "file2.eml"}])
        mocker.patch("mailbackup.utils.rclone_lsjson", return_value=Mock(
            returncode=0,
            stdout=lsjson_output
        ))

        # Mock compute_remote_sha256 to fail for one file
        def mock_compute(settings, path):
            if path == "file1.eml":
                raise Exception("Compute failed")
            return "hash_ok"

        mocker.patch("mailbackup.utils.compute_remote_sha256", side_effect=mock_compute)

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is not None
        # Only successful file should be in result
        assert "file2.eml" in result
        assert result["file2.eml"] == "hash_ok"


@pytest.mark.integration
class TestUtilityFunctionsIntegration:
    """Integration tests for utility functions."""

    def test_working_dir(self, tmp_path):
        """Test working_dir context manager."""
        original = Path.cwd()
        new_dir = tmp_path / "workdir"
        new_dir.mkdir()

        with working_dir(new_dir):
            assert Path.cwd() == new_dir

        assert Path.cwd() == original

    def test_working_dir_exception(self, tmp_path):
        """Test working_dir restores directory even on exception."""
        original = Path.cwd()
        new_dir = tmp_path / "workdir"
        new_dir.mkdir()

        try:
            with working_dir(new_dir):
                assert Path.cwd() == new_dir
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert Path.cwd() == original

    def test_ensure_dirs(self, tmp_path):
        """Test ensure_dirs creates multiple directories."""
        dir1 = tmp_path / "dir1" / "subdir1"
        dir2 = tmp_path / "dir2" / "subdir2"
        dir3 = tmp_path / "dir3"

        ensure_dirs(dir1, dir2, dir3)

        assert dir1.exists()
        assert dir2.exists()
        assert dir3.exists()

    def test_install_signal_handlers(self):
        """Test install_signal_handlers sets up handlers."""
        handler_called = []

        def test_handler(signum, frame):
            handler_called.append(signum)

        install_signal_handlers(test_handler)

        # Verify handlers are installed by checking signal.getsignal
        assert signal.getsignal(signal.SIGINT) == test_handler
        assert signal.getsignal(signal.SIGTERM) == test_handler


@pytest.mark.integration
class TestStatusThreadIntegration:
    """Integration tests for StatusThread."""

    def test_status_thread_starts_and_stops(self):
        """Test StatusThread lifecycle."""
        
        counters = create_stats()
        counters.increment(StatKey.BACKED_UP, 5)
        counters.increment(StatKey.VERIFIED, 10)
        thread = StatusThread(interval=1, counters=counters)

        # Should not be running yet
        assert thread._thread is None

        thread.start()
        assert thread._thread is not None
        assert thread._thread.is_alive()

        # Wait a moment for thread to run
        time.sleep(0.1)

        thread.stop()
        time.sleep(0.5)

        # Thread should be stopped
        assert not thread._thread.is_alive()

    def test_status_thread_multiple_start_calls(self):
        """Test that calling start multiple times is safe."""
        counters = {"uploaded": 0}
        thread = StatusThread(interval=1, counters=counters)

        thread.start()
        first_thread = thread._thread

        # Second start should not create new thread
        thread.start()
        assert thread._thread is first_thread

        thread.stop()

    def test_status_thread_stop_before_start(self):
        """Test that stopping before starting is safe."""
        counters = {"uploaded": 0}
        thread = StatusThread(interval=1, counters=counters)

        # Should not raise exception
        thread.stop()
        assert thread._thread is None

    def test_status_thread_with_custom_logger(self, mocker):
        """Test StatusThread uses status method when available."""
        
        counters = create_stats()
        counters.increment(StatKey.BACKED_UP, 5)
        counters.increment(StatKey.VERIFIED, 10)

        # Mock logger with status method
        mock_logger = Mock()
        mock_logger.status = Mock()
        mock_logger.info = Mock()

        thread = StatusThread(interval=0.1, counters=counters)
        thread.logger = mock_logger

        thread.start()
        time.sleep(0.3)
        thread.stop()

        # Either status or info should have been called
        assert mock_logger.status.called or mock_logger.info.called

    def test_status_thread_fallback_to_info(self, mocker):
        """Test StatusThread falls back to info when status not available."""
        counters = create_stats()
        counters.increment(StatKey.BACKED_UP, 5)

        # Mock logger without status method
        mock_logger = Mock()
        mock_logger.info = Mock()
        # Make sure status is not callable
        mock_logger.status = None

        thread = StatusThread(interval=0.1, counters=counters)
        thread.logger = mock_logger

        thread.start()
        time.sleep(0.3)
        thread.stop()

        # info should have been called as fallback
        assert mock_logger.info.called
