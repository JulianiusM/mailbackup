#!/usr/bin/env python3
"""
Unit tests for utils.py module.
"""

import datetime
import hashlib
import json
from pathlib import Path

from mailbackup.statistics import StatusThread, create_stats, StatKey
from mailbackup.utils import (
    sanitize,
    sha256,
    sha256_bytes,
    parse_mail_date,
    date_iso,
    parse_year_and_ts,
    write_json_atomic,
    safe_write_json,
    atomic_write_text,
    unique_path_for_filename,
    load_attachments,
    build_info_json,
)


class TestSanitize:
    """Tests for sanitize function."""

    def test_sanitize_basic_string(self):
        assert sanitize("hello world") == "hello_world"

    def test_sanitize_removes_special_chars(self):
        result = sanitize('test<>:"/\\|?*file')
        assert "<" not in result
        assert ">" not in result
        assert "/" not in result
        assert "\\" not in result

    def test_sanitize_none_returns_unknown(self):
        assert sanitize(None) == "unknown"

    def test_sanitize_empty_string_returns_unknown(self):
        assert sanitize("") == "unknown"

    def test_sanitize_truncates_long_strings(self):
        long_string = "a" * 200
        result = sanitize(long_string)
        assert len(result) == 80

    def test_sanitize_unicode_normalization(self):
        # Test that Unicode is normalized and converted to ASCII
        result = sanitize("café")
        assert result == "cafe"


class TestSHA256:
    """Tests for SHA256 functions."""

    def test_sha256_file(self, tmp_path):
        test_file = tmp_path / "test.txt"
        content = b"Hello, World!"
        test_file.write_bytes(content)

        result = sha256(test_file)
        expected = hashlib.sha256(content).hexdigest()
        assert result == expected

    def test_sha256_bytes(self):
        data = b"test data"
        result = sha256_bytes(data)
        expected = hashlib.sha256(data).hexdigest()
        assert result == expected

    def test_sha256_empty_file(self, tmp_path):
        test_file = tmp_path / "empty.txt"
        test_file.write_bytes(b"")

        result = sha256(test_file)
        expected = hashlib.sha256(b"").hexdigest()
        assert result == expected


class TestDateParsing:
    """Tests for date parsing functions."""

    def test_parse_mail_date_iso_format(self):
        date_str = "2024-01-15T10:30:00+00:00"
        result = parse_mail_date(date_str)
        assert isinstance(result, datetime.datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_mail_date_rfc2822_format(self):
        date_str = "Mon, 15 Jan 2024 10:30:00 +0000"
        result = parse_mail_date(date_str)
        assert isinstance(result, datetime.datetime)
        assert result.year == 2024

    def test_parse_mail_date_none_returns_current(self):
        result = parse_mail_date(None)
        assert isinstance(result, datetime.datetime)
        # Should be close to now
        now = datetime.datetime.now(datetime.timezone.utc)
        assert abs((result - now).total_seconds()) < 2

    def test_parse_mail_date_with_comment(self):
        # Some mail dates have comments in parentheses
        date_str = "Mon, 15 Jan 2024 10:30:00 +0000 (UTC)"
        result = parse_mail_date(date_str)
        assert isinstance(result, datetime.datetime)
        assert result.year == 2024

    def test_date_iso(self):
        date_str = "Mon, 15 Jan 2024 10:30:00 +0000"
        result = date_iso(date_str)
        assert isinstance(result, str)
        assert "2024" in result

    def test_parse_year_and_ts(self):
        date_str = "Mon, 15 Jan 2024 10:30:00 +0000"
        year, ts = parse_year_and_ts(date_str)
        assert year == 2024
        assert isinstance(ts, str)
        assert "2024-01-15" in ts


class TestJSONWriting:
    """Tests for JSON writing functions."""

    def test_write_json_atomic(self, tmp_path):
        test_file = tmp_path / "test.json"
        data = {"key": "value", "number": 42}

        write_json_atomic(test_file, data)

        assert test_file.exists()
        loaded = json.loads(test_file.read_text())
        assert loaded == data

    def test_write_json_atomic_overwrites_existing(self, tmp_path):
        test_file = tmp_path / "test.json"
        test_file.write_text('{"old": "data"}')

        new_data = {"new": "data"}
        write_json_atomic(test_file, new_data)

        loaded = json.loads(test_file.read_text())
        assert loaded == new_data

    def test_safe_write_json(self, tmp_path):
        test_file = tmp_path / "test.json"
        data = {"test": True}

        safe_write_json(test_file, data)

        assert test_file.exists()
        loaded = json.loads(test_file.read_text())
        assert loaded == data

    def test_atomic_write_text_string(self, tmp_path):
        test_file = tmp_path / "test.txt"
        content = "Hello, World!"

        atomic_write_text(test_file, content)

        assert test_file.exists()
        assert test_file.read_text() == content

    def test_atomic_write_text_iterable(self, tmp_path):
        test_file = tmp_path / "test.txt"
        lines = ["line1\n", "line2\n", "line3\n"]

        atomic_write_text(test_file, lines)

        assert test_file.exists()
        assert test_file.read_text() == "line1\nline2\nline3\n"


class TestUniquePathForFilename:
    """Tests for unique_path_for_filename function."""

    def test_unique_path_no_collision(self, tmp_path):
        result = unique_path_for_filename(tmp_path, "test.txt")
        assert result == tmp_path / "test.txt"

    def test_unique_path_with_collision(self, tmp_path):
        # Create existing file
        existing = tmp_path / "test.txt"
        existing.touch()

        result = unique_path_for_filename(tmp_path, "test.txt")
        assert result == tmp_path / "test-1.txt"

    def test_unique_path_multiple_collisions(self, tmp_path):
        # Create multiple existing files
        (tmp_path / "test.txt").touch()
        (tmp_path / "test-1.txt").touch()
        (tmp_path / "test-2.txt").touch()

        result = unique_path_for_filename(tmp_path, "test.txt")
        assert result == tmp_path / "test-3.txt"


class TestLoadAttachments:
    """Tests for load_attachments function."""

    def test_load_attachments_valid_json(self):
        json_str = '["/path/to/file1.pdf", "/path/to/file2.txt"]'
        result = load_attachments(json_str)
        assert len(result) == 2
        assert isinstance(result[0], Path)
        assert str(result[0]) == "/path/to/file1.pdf"

    def test_load_attachments_none(self):
        result = load_attachments(None)
        assert result == []

    def test_load_attachments_empty_string(self):
        result = load_attachments("")
        assert result == []

    def test_load_attachments_invalid_json(self):
        result = load_attachments("{invalid json}")
        assert result == []

    def test_load_attachments_non_list(self):
        result = load_attachments('{"not": "a list"}')
        assert result == []


class TestBuildInfoJson:
    """Tests for build_info_json function."""

    def test_build_info_json_with_dict(self):
        row = {
            "id": 123,
            "hash": "abc123",
            "path": "/path/to/email.eml",
            "from_header": "test@example.com",
            "subject": "Test Subject",
            "date_header": "2024-01-15 10:30:00",
            "spam": 0,
            "processed_at": "2024-01-15T10:30:00",
        }

        result = build_info_json(
            row=row,
            att_names=["file1.pdf", "file2.txt"],
            hash_email="sha256hash",
            remote_path="remote/path/email.eml"
        )

        assert result["id"] == 123
        assert result["hash"] == "abc123"
        assert result["hash_sha256"] == "sha256hash"
        assert result["remote_path"] == "remote/path/email.eml"
        assert result["attachments"] == ["file1.pdf", "file2.txt"]
        assert "synced_at" in result

    def test_build_info_json_handles_none_values(self):
        row = {"id": None, "hash": None, "path": None}

        result = build_info_json(
            row=row,
            att_names=[],
            hash_email="hash",
            remote_path="path"
        )

        assert result["id"] is None
        assert result["hash"] is None


class TestStatusThread:
    """Tests for StatusThread class."""

    def test_status_thread_initialization(self):
        stats = create_stats()
        thread = StatusThread(interval=10, counters=stats)
        assert thread.interval == 10
        assert thread.counters == stats

    def test_status_summary(self):
        stats = create_stats()
        stats.set(StatKey.BACKED_UP, 5)
        stats.set(StatKey.ARCHIVED, 3)
        stats.set(StatKey.VERIFIED, 2)
        stats.set(StatKey.REPAIRED, 1)
        stats.set(StatKey.SKIPPED, 0)
        thread = StatusThread(interval=10, counters=stats)
        summary = thread.get_status_summary()

        assert "Backed up: 5" in summary
        assert "Archived: 3" in summary
        assert "Verified: 2" in summary

    def test_status_thread_start_stop(self):
        thread = StatusThread(interval=1, counters=create_stats())

        thread.start()
        assert thread._thread is not None

        thread.stop()
        # Thread should be stopped


class TestAtomicOperations:
    """Tests for atomic file operations."""

    def test_atomic_write_text_creates_file(self, tmp_path):
        """Test atomic_write_text creates file successfully."""
        from mailbackup.utils import atomic_write_text
        
        file_path = tmp_path / "test.txt"
        content = "Test content\n"
        
        atomic_write_text(file_path, content)
        
        assert file_path.exists()
        assert file_path.read_text() == content

    def test_atomic_write_text_with_list(self, tmp_path):
        """Test atomic_write_text with list of lines."""
        from mailbackup.utils import atomic_write_text
        
        file_path = tmp_path / "test.txt"
        content = ["Line 1\n", "Line 2\n", "Line 3\n"]
        
        atomic_write_text(file_path, content)
        
        assert file_path.exists()
        assert file_path.read_text() == "Line 1\nLine 2\nLine 3\n"


class TestDateParsing:
    """Tests for date parsing functions."""

    def test_parse_mail_date_rfc2822(self):
        """Test parse_mail_date with RFC 2822 format."""
        from mailbackup.utils import parse_mail_date
        
        date_str = "Mon, 1 Jan 2024 12:00:00 +0000"
        result = parse_mail_date(date_str)
        
        assert result is not None
        assert result.year == 2024

    def test_parse_mail_date_iso_format(self):
        """Test parse_mail_date with ISO format."""
        from mailbackup.utils import parse_mail_date
        
        date_str = "2024-01-01T12:00:00"
        result = parse_mail_date(date_str)
        
        assert result is not None
        assert result.year == 2024

    def test_parse_mail_date_invalid(self):
        """Test parse_mail_date with invalid date."""
        from mailbackup.utils import parse_mail_date
        
        date_str = "not a valid date"
        result = parse_mail_date(date_str)
        
        # Should return current time
        assert result is not None

    def test_parse_mail_date_empty(self):
        """Test parse_mail_date with empty string."""
        from mailbackup.utils import parse_mail_date
        
        result = parse_mail_date("")
        
        # Should return current time
        assert result is not None


class TestRemoteHashOperations:
    """Tests for remote hash computation."""

    def test_compute_remote_sha256_success(self, test_settings, mocker):
        """Test compute_remote_sha256 with successful hash computation."""
        from mailbackup.utils import compute_remote_sha256
        
        # Mock rclone_cat to return content
        mocker.patch("mailbackup.utils.rclone_cat", return_value=mocker.Mock(
            returncode=0,
            stdout="test content"
        ))
        
        result = compute_remote_sha256(test_settings, "path/to/file.txt")
        
        # Should return a SHA256 hash
        assert result != ""
        assert len(result) == 64  # SHA256 hex length

    def test_compute_remote_sha256_failure(self, test_settings, mocker):
        """Test compute_remote_sha256 when rclone fails."""
        from mailbackup.utils import compute_remote_sha256
        
        # Mock rclone_cat to fail
        mocker.patch("mailbackup.utils.rclone_cat", side_effect=Exception("rclone failed"))
        
        result = compute_remote_sha256(test_settings, "path/to/file.txt")
        
        # Should return empty string
        assert result == ""

    def test_compute_remote_sha256_exception(self, test_settings, mocker):
        """Test compute_remote_sha256 when exception occurs."""
        from mailbackup.utils import compute_remote_sha256
        
        # Mock rclone_cat to raise exception
        mocker.patch("mailbackup.utils.rclone_cat", side_effect=Exception("Error"))
        
        result = compute_remote_sha256(test_settings, "path/to/file.txt")
        
        # Should return empty string
        assert result == ""


class TestWorkingDirectory:
    """Tests for working_dir context manager."""

    def test_working_dir_changes_directory(self, tmp_path):
        """Test working_dir context manager changes directory."""
        from mailbackup.utils import working_dir
        import os
        
        original_dir = os.getcwd()
        test_dir = tmp_path / "testdir"
        test_dir.mkdir()
        
        with working_dir(test_dir):
            assert os.getcwd() == str(test_dir)
        
        # Should restore original directory
        assert os.getcwd() == original_dir

    def test_working_dir_exception_restores(self, tmp_path):
        """Test working_dir restores directory even on exception."""
        from mailbackup.utils import working_dir
        import os
        
        original_dir = os.getcwd()
        test_dir = tmp_path / "testdir"
        test_dir.mkdir()
        
        try:
            with working_dir(test_dir):
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Should still restore original directory
        assert os.getcwd() == original_dir


class TestEnsureDirs:
    """Tests for ensure_dirs function."""

    def test_ensure_dirs_creates_directories(self, tmp_path):
        """Test ensure_dirs creates multiple directories."""
        from mailbackup.utils import ensure_dirs
        
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir3 = tmp_path / "dir3"
        
        ensure_dirs(dir1, dir2, dir3)
        
        assert dir1.exists() and dir1.is_dir()
        assert dir2.exists() and dir2.is_dir()
        assert dir3.exists() and dir3.is_dir()

    def test_ensure_dirs_existing_directories(self, tmp_path):
        """Test ensure_dirs with existing directories."""
        from mailbackup.utils import ensure_dirs
        
        dir1 = tmp_path / "dir1"
        dir1.mkdir()
        
        # Should not raise error
        ensure_dirs(dir1)
        
        assert dir1.exists()
