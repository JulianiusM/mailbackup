#!/usr/bin/env python3
"""
Comprehensive integration tests to achieve 94%+ code coverage.
Focuses on real integration scenarios without mocking internal code bugs.
"""

import json
from mailbackup.statistics import StatKey, create_stats
import signal
from mailbackup.statistics import StatKey, create_stats
import subprocess
from mailbackup.statistics import StatKey, create_stats
import time
from mailbackup.statistics import StatKey, create_stats
from pathlib import Path
from unittest.mock import Mock

import pytest
from mailbackup.statistics import StatKey, create_stats

from mailbackup.extractor import (
    decode_mime_header,
    decode_text_part,
    run_extractor,
)
from mailbackup.integrity import (
    rebuild_docset,
    integrity_check,
)
from mailbackup.manifest import ManifestManager
from mailbackup.statistics import StatusThread
from mailbackup.utils import (
    run_streaming,
    atomic_upload_file,
    compute_remote_sha256,
    remote_hash,
    working_dir,
    ensure_dirs,
    install_signal_handlers,
    run_cmd,
    atomic_write_text,
    parse_mail_date,
)


@pytest.mark.integration
class TestUtilsIntegration:
    """Integration tests for utils module."""

    def test_run_cmd_with_fatal_exception(self):
        """Test run_cmd raises when fatal=True and command fails."""
        with pytest.raises(subprocess.CalledProcessError):
            run_cmd("false", fatal=True)

    def test_run_cmd_non_fatal_failure(self):
        """Test run_cmd returns error when fatal=False."""
        result = run_cmd("false", check=True, fatal=False)
        assert isinstance(result, subprocess.CalledProcessError)

    def test_run_streaming_success(self):
        """Test run_streaming with successful command."""
        result = run_streaming("echo", ["echo", "test"], ignore_errors=True)
        assert result is True

    def test_run_streaming_failure_not_raised(self):
        """Test run_streaming returns False when ignore_errors=True."""
        result = run_streaming("false", ["false"], ignore_errors=True)
        assert result is False

    def test_run_streaming_failure_raised(self):
        """Test run_streaming raises when ignore_errors=False."""
        with pytest.raises(subprocess.CalledProcessError):
            run_streaming("false", ["false"], ignore_errors=False)

    def test_atomic_write_text_with_string(self, tmp_path):
        """Test atomic_write_text with string data."""
        file_path = tmp_path / "test.txt"
        atomic_write_text(file_path, "test content")
        assert file_path.read_text() == "test content"

    def test_atomic_write_text_with_lines(self, tmp_path):
        """Test atomic_write_text with list of lines."""
        file_path = tmp_path / "test.txt"
        atomic_write_text(file_path, ["line1\n", "line2\n"])
        assert file_path.read_text() == "line1\nline2\n"

    def test_atomic_write_text_cleanup_on_error(self, tmp_path, mocker):
        """Test atomic_write_text cleans up temp file on error."""
        file_path = tmp_path / "test.txt"
        mocker.patch("os.fsync", side_effect=Exception("Error"))

        with pytest.raises(Exception):
            atomic_write_text(file_path, "test")

        # Verify temp file was cleaned up
        assert not list(tmp_path.glob("*.tmp"))

    def test_atomic_upload_file_success(self, tmp_path, mocker):
        """Test atomic_upload_file with successful upload."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("content")

        mocker.patch("mailbackup.utils.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.utils.rclone_moveto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.utils.rclone_deletefile", return_value=Mock(returncode=0))

        result = atomic_upload_file(local_file, "remote:path/file.txt")
        assert result is True

    def test_atomic_upload_file_copyto_failure(self, tmp_path, mocker):
        """Test atomic_upload_file when copyto fails."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("content")

        mocker.patch("mailbackup.utils.rclone_copyto", return_value=Mock(returncode=1))
        mock_delete = mocker.patch("mailbackup.utils.rclone_deletefile", return_value=Mock(returncode=0))

        result = atomic_upload_file(local_file, "remote:path/file.txt")
        assert result is False
        mock_delete.assert_called_once()

    def test_atomic_upload_file_moveto_failure(self, tmp_path, mocker):
        """Test atomic_upload_file when moveto fails."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("content")

        mocker.patch("mailbackup.utils.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.utils.rclone_moveto", return_value=Mock(returncode=1))
        mock_delete = mocker.patch("mailbackup.utils.rclone_deletefile", return_value=Mock(returncode=0))

        result = atomic_upload_file(local_file, "remote:path/file.txt")
        assert result is False
        mock_delete.assert_called_once()

    def test_compute_remote_sha256_success(self, test_settings, mocker):
        """Test compute_remote_sha256 with successful rclone_cat."""
        mocker.patch("mailbackup.utils.rclone_cat", return_value=Mock(
            returncode=0,
            stdout="test content"
        ))

        result = compute_remote_sha256(test_settings, "file.txt")
        assert len(result) == 64

    def test_compute_remote_sha256_exception(self, test_settings, mocker):
        """Test compute_remote_sha256 when exception occurs."""
        mocker.patch("mailbackup.utils.rclone_cat", side_effect=Exception("Failed"))

        result = compute_remote_sha256(test_settings, "file.txt")
        assert result == ""

    def test_remote_hash_with_hashsum_support(self, test_settings, mocker):
        """Test remote_hash when hashsum is supported."""
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(
            returncode=0,
            stdout="abc123  file1.eml\ndef456  file2.eml\n"
        ))

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is not None
        assert len(result) == 2

    def test_remote_hash_fallback_streaming(self, test_settings, mocker):
        """Test remote_hash fallback when hashsum not available."""
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(returncode=1, stdout=""))
        mocker.patch("mailbackup.utils.rclone_lsjson", return_value=Mock(
            returncode=0,
            stdout=json.dumps([{"Path": "file1.eml"}])
        ))
        mocker.patch("mailbackup.utils.compute_remote_sha256", return_value="hash123")

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is not None

    def test_remote_hash_lsjson_failure(self, test_settings, mocker):
        """Test remote_hash when lsjson fails."""
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(returncode=1, stdout=""))
        mocker.patch("mailbackup.utils.rclone_lsjson", return_value=Mock(returncode=1, stdout=""))

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is None

    def test_remote_hash_compute_exception_handling(self, test_settings, mocker):
        """Test remote_hash handles compute_remote_sha256 exceptions."""
        mocker.patch("mailbackup.utils.rclone_hashsum", return_value=Mock(returncode=1, stdout=""))
        mocker.patch("mailbackup.utils.rclone_lsjson", return_value=Mock(
            returncode=0,
            stdout=json.dumps([{"Path": "file1.eml"}])
        ))
        mocker.patch("mailbackup.utils.compute_remote_sha256", side_effect=Exception("Error"))

        result = remote_hash(test_settings, "*.eml", silent_logging=True)
        assert result is not None

    def test_working_dir_context_manager(self, tmp_path):
        """Test working_dir context manager."""
        original = Path.cwd()
        new_dir = tmp_path / "workdir"
        new_dir.mkdir()

        with working_dir(new_dir):
            assert Path.cwd() == new_dir

        assert Path.cwd() == original

    def test_ensure_dirs_multiple(self, tmp_path):
        """Test ensure_dirs creates multiple directories."""
        dir1 = tmp_path / "dir1" / "sub1"
        dir2 = tmp_path / "dir2" / "sub2"

        ensure_dirs(dir1, dir2)

        assert dir1.exists()
        assert dir2.exists()

    def test_install_signal_handlers_sets_handlers(self):
        """Test install_signal_handlers sets signal handlers."""

        def handler(signum, frame):
            pass

        install_signal_handlers(handler)

        assert signal.getsignal(signal.SIGINT) == handler
        assert signal.getsignal(signal.SIGTERM) == handler

    def test_status_thread_lifecycle(self):
        """Test StatusThread start, run, and stop."""
        counters = {"uploaded": 5}
        thread = StatusThread(interval=1, counters=counters)

        thread.start()
        assert thread._thread is not None
        assert thread._thread.is_alive()

        time.sleep(0.2)
        thread.stop()
        time.sleep(0.5)

        assert not thread._thread.is_alive()

    def test_status_thread_multiple_start_safe(self):
        """Test calling start() multiple times is safe."""
        counters = {"uploaded": 0}
        thread = StatusThread(interval=1, counters=counters)

        thread.start()
        first_thread = thread._thread
        thread.start()

        assert thread._thread is first_thread
        thread.stop()

    def test_parse_mail_date_iso_format(self):
        """Test parse_mail_date with ISO format."""
        result = parse_mail_date("2024-01-01T12:00:00+00:00")
        assert result.year == 2024

    def test_parse_mail_date_rfc_format(self):
        """Test parse_mail_date with RFC format."""
        result = parse_mail_date("Mon, 1 Jan 2024 12:00:00 +0000")
        assert result.year == 2024

    def test_parse_mail_date_invalid_fallback(self):
        """Test parse_mail_date falls back to current time for invalid input."""
        result = parse_mail_date("invalid date")
        # Should return current time, just check it's valid
        assert result.year >= 2024


@pytest.mark.integration
class TestExtractorIntegration:
    """Integration tests for extractor module."""

    def test_decode_mime_header_none(self):
        """Test decode_mime_header with None."""
        assert decode_mime_header(None) == ""

    def test_decode_mime_header_empty(self):
        """Test decode_mime_header with empty string."""
        assert decode_mime_header("") == ""

    def test_decode_mime_header_normal(self):
        """Test decode_mime_header with normal string."""
        result = decode_mime_header("Test Subject")
        assert "Test" in result

    def test_decode_text_part_no_payload(self):
        """Test decode_text_part with no payload."""
        import email
        part = email.message.EmailMessage()
        result = decode_text_part(part)
        assert result == ""

    def test_decode_text_part_with_charset(self):
        """Test decode_text_part with UTF-8 charset."""
        import email
        part = email.message.EmailMessage()
        part.set_content("Test content", charset="utf-8")
        result = decode_text_part(part)
        assert "Test" in result

    def test_run_extractor_nonexistent_maildir(self, test_settings):
        """Test run_extractor with nonexistent maildir."""
        test_settings.maildir = Path("/nonexistent")
        stats = {"extracted": 0}

        run_extractor(test_settings, stats)

        assert stats[StatKey.EXTRACTED] == 0


@pytest.mark.integration
class TestIntegrityIntegration:
    """Integration tests for integrity module."""

    def test_rebuild_docset_missing_mail_file(self, test_settings):
        """Test rebuild_docset when mail file doesn't exist."""
        import shutil

        row = {
            "id": 1,
            "hash": "abc123",
            "path": "/nonexistent/mail.eml",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        result = rebuild_docset(test_settings, 2024, "test_folder", row)

        assert result.exists()
        assert (result / "info.json").exists()
        assert not (result / "email.eml").exists()

        shutil.rmtree(result, ignore_errors=True)

    def test_integrity_check_disabled(self, test_settings):
        """Test integrity_check when verification is disabled."""
        test_settings.verify_integrity = False

        manifest = Mock(spec=ManifestManager)
        stats = {"verified": 0}

        integrity_check(test_settings, manifest, stats)

        assert stats[StatKey.VERIFIED] == 0

    def test_integrity_check_no_remote_hash(self, test_settings, mocker):
        """Test integrity_check when remote_hash returns None."""
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.integrity.remote_hash", return_value=None)
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[])

        manifest = Mock(spec=ManifestManager)
        stats = {"verified": 0}

        integrity_check(test_settings, manifest, stats)

        assert stats[StatKey.VERIFIED] == 0


@pytest.mark.integration
class TestManifestIntegration:
    """Integration tests for manifest module."""

    def test_manifest_manager_queue_and_dump(self, test_settings):
        """Test ManifestManager queuing and dumping."""
        manager = ManifestManager(test_settings)

        manager.queue_entry("path1.eml", "hash1")
        manager.queue_entry("path2.eml", "hash2")
        manager.dump_queue()

        assert manager.manifest_queue_dump.exists()

        # Verify content
        data = json.loads(manager.manifest_queue_dump.read_text())
        assert "path1.eml" in data
        assert "path2.eml" in data

    def test_manifest_manager_upload_if_needed(self, test_settings, mocker):
        """Test upload_manifest_if_needed."""
        mocker.patch("mailbackup.manifest.rclone_copyto", return_value=Mock(returncode=0))
        mocker.patch("mailbackup.manifest.rclone_lsjson", return_value=Mock(returncode=1, stdout="[]"))
        mocker.patch("mailbackup.manifest.rclone_moveto", return_value=Mock(returncode=0))

        manager = ManifestManager(test_settings)
        manager.queue_entry("path.eml", "hash")

        # Should upload since there's a queued entry
        manager.upload_manifest_if_needed()
