#!/usr/bin/env python3
"""
Unit tests for utils.py module.
"""

import pytest
import json
import datetime
import hashlib
from pathlib import Path
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
    StatusThread,
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
        # Test that unicode is normalized and converted to ASCII
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
        counters = {"uploaded": 0, "archived": 0}
        thread = StatusThread(interval=10, counters=counters)
        assert thread.interval == 10
        assert thread.counters == counters
    
    def test_status_summary(self):
        counters = {
            "uploaded": 5,
            "archived": 3,
            "verified": 2,
            "repaired": 1,
            "skipped": 0
        }
        thread = StatusThread(interval=10, counters=counters)
        summary = thread.get_status_summary()
        
        assert "Uploaded: 5" in summary
        assert "Archived: 3" in summary
        assert "Verified: 2" in summary
    
    def test_status_thread_start_stop(self):
        counters = {"uploaded": 0}
        thread = StatusThread(interval=1, counters=counters)
        
        thread.start()
        assert thread._thread is not None
        
        thread.stop()
        # Thread should be stopped
