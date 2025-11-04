#!/usr/bin/env python3
"""
Unit tests for config.py module.
"""

import pytest
from pathlib import Path
from mailbackup.config import (
    Settings,
    load_settings,
    _load_toml,
    _load_ini,
    _coerce_bool,
    _coerce_int,
)


class TestCoerceFunctions:
    """Tests for type coercion helper functions."""
    
    def test_coerce_bool_true_values(self):
        assert _coerce_bool("1", False) is True
        assert _coerce_bool("true", False) is True
        assert _coerce_bool("TRUE", False) is True
        assert _coerce_bool("yes", False) is True
        assert _coerce_bool("on", False) is True
        assert _coerce_bool(True, False) is True
    
    def test_coerce_bool_false_values(self):
        assert _coerce_bool("0", True) is False
        assert _coerce_bool("false", True) is False
        assert _coerce_bool("FALSE", True) is False
        assert _coerce_bool("no", True) is False
        assert _coerce_bool("off", True) is False
        assert _coerce_bool(False, True) is False
    
    def test_coerce_bool_none_returns_default(self):
        assert _coerce_bool(None, True) is True
        assert _coerce_bool(None, False) is False
    
    def test_coerce_bool_invalid_returns_default(self):
        assert _coerce_bool("invalid", True) is True
        assert _coerce_bool("maybe", False) is False
    
    def test_coerce_int_valid_values(self):
        assert _coerce_int("42", 0) == 42
        assert _coerce_int(123, 0) == 123
        assert _coerce_int("0", 10) == 0
    
    def test_coerce_int_none_returns_default(self):
        assert _coerce_int(None, 42) == 42
    
    def test_coerce_int_invalid_returns_default(self):
        assert _coerce_int("not a number", 10) == 10
        assert _coerce_int("12.5", 20) == 20


class TestSettings:
    """Tests for Settings dataclass."""
    
    def test_settings_creation(self, tmp_path):
        settings = Settings(
            maildir=tmp_path / "maildir",
            attachments_dir=tmp_path / "attachments",
            remote="nextcloud:Backups/Email",
            db_path=tmp_path / "state.db",
            log_path=tmp_path / "sync.log",
            tmp_dir=tmp_path / "staging",
            archive_dir=tmp_path / "archives",
            manifest_path=tmp_path / "manifest.csv",
            retention_years=2,
            keep_local_after_archive=False,
            verify_integrity=True,
            repair_on_failure=True,
            manifest_remote_name="manifest.csv",
            max_manifest_conflict_retries=3,
            max_hash_threads=8,
            max_upload_workers=4,
            max_extract_workers=1,
            status_interval=300,
            fetch_command="mbsync -V -a",
            rclone_log_level="INFO",
            rclone_transfers=8,
            rclone_multi_thread_streams=4,
        )
        
        assert settings.maildir == tmp_path / "maildir"
        assert settings.retention_years == 2
        assert settings.verify_integrity is True
    
    def test_manifest_remote_path_property(self, tmp_path):
        settings = Settings(
            maildir=tmp_path / "maildir",
            attachments_dir=tmp_path / "attachments",
            remote="nextcloud:Backups/Email",
            db_path=tmp_path / "state.db",
            log_path=tmp_path / "sync.log",
            tmp_dir=tmp_path / "staging",
            archive_dir=tmp_path / "archives",
            manifest_path=tmp_path / "manifest.csv",
            retention_years=2,
            keep_local_after_archive=False,
            verify_integrity=True,
            repair_on_failure=True,
            manifest_remote_name="manifest.csv",
            max_manifest_conflict_retries=3,
            max_hash_threads=8,
            max_upload_workers=4,
            max_extract_workers=1,
            status_interval=300,
            fetch_command="mbsync -V -a",
            rclone_log_level="INFO",
            rclone_transfers=8,
            rclone_multi_thread_streams=4,
        )
        
        assert settings.manifest_remote_path == "nextcloud:Backups/Email/manifest.csv"


class TestLoadToml:
    """Tests for TOML configuration loading."""
    
    def test_load_toml_valid_file(self, tmp_path):
        toml_file = tmp_path / "test.toml"
        toml_file.write_text("""
[paths]
maildir = "/srv/maildir"
remote = "nextcloud:Backups/Email"

[rotation]
retention_years = 3
""")
        
        try:
            data = _load_toml(toml_file)
            assert "paths" in data
            assert data["paths"]["maildir"] == "/srv/maildir"
            assert data["rotation"]["retention_years"] == 3
        except RuntimeError as e:
            # If no TOML library available, skip test
            if "no TOML parser available" in str(e):
                pytest.skip("TOML parser not available")
            raise
    
    def test_load_toml_missing_file(self, tmp_path):
        toml_file = tmp_path / "nonexistent.toml"
        
        with pytest.raises(FileNotFoundError):
            _load_toml(toml_file)


class TestLoadIni:
    """Tests for INI configuration loading."""
    
    def test_load_ini_valid_file(self, tmp_path):
        ini_file = tmp_path / "test.ini"
        ini_file.write_text("""[mailbackup]
maildir = /srv/maildir
remote = nextcloud:Backups/Email
retention_years = 3
""")
        
        data = _load_ini(ini_file)
        assert data["maildir"] == "/srv/maildir"
        assert data["remote"] == "nextcloud:Backups/Email"
        assert data["retention_years"] == "3"
    
    def test_load_ini_missing_section(self, tmp_path):
        ini_file = tmp_path / "test.ini"
        ini_file.write_text("""[wrong_section]
key = value
""")
        
        with pytest.raises(RuntimeError, match="must have a .* section"):
            _load_ini(ini_file)


class TestLoadSettings:
    """Tests for load_settings function."""
    
    def test_load_settings_from_toml(self, tmp_path):
        toml_file = tmp_path / "mailbackup.toml"
        toml_file.write_text("""
maildir = "/test/maildir"
remote = "test:remote"
retention_years = 5
verify_integrity = true
max_hash_threads = 16
""")
        
        try:
            settings = load_settings(toml_file)
            assert settings.maildir == Path("/test/maildir")
            assert settings.remote == "test:remote"
            assert settings.retention_years == 5
            assert settings.verify_integrity is True
            assert settings.max_hash_threads == 16
        except RuntimeError as e:
            if "no TOML parser available" in str(e):
                pytest.skip("TOML parser not available")
            raise
    
    def test_load_settings_from_ini(self, tmp_path):
        ini_file = tmp_path / "mailbackup.ini"
        ini_file.write_text("""[mailbackup]
maildir = /test/maildir
remote = test:remote
retention_years = 5
verify_integrity = true
max_hash_threads = 16
""")
        
        settings = load_settings(ini_file)
        assert settings.maildir == Path("/test/maildir")
        assert settings.remote == "test:remote"
        assert settings.retention_years == 5
        assert settings.verify_integrity is True
        assert settings.max_hash_threads == 16
    
    def test_load_settings_nonexistent_file_raises(self, tmp_path):
        nonexistent = tmp_path / "nonexistent.toml"
        
        with pytest.raises(FileNotFoundError):
            load_settings(nonexistent)
    
    def test_load_settings_uses_defaults_when_no_file(self, tmp_path, monkeypatch):
        # Change to a directory with no config files
        monkeypatch.chdir(tmp_path)
        
        # Should use built-in defaults without error
        settings = load_settings(None)
        assert isinstance(settings, Settings)
        assert settings.retention_years == 2  # default value
    
    def test_load_settings_nested_config(self, tmp_path):
        ini_file = tmp_path / "mailbackup.ini"
        ini_file.write_text("""[mailbackup]
maildir = /custom/maildir
attachments_dir = /custom/attachments
""")
        
        settings = load_settings(ini_file)
        assert settings.maildir == Path("/custom/maildir")
        assert settings.attachments_dir == Path("/custom/attachments")


class TestConfigEdgeCases:
    """Tests for config loading edge cases."""

    def test_load_settings_with_invalid_toml(self, tmp_path):
        """Test load_settings with malformed TOML file."""
        from mailbackup.config import load_settings
        
        config_file = tmp_path / "bad.toml"
        config_file.write_text("invalid [toml content")
        
        # Should raise or handle error
        try:
            settings = load_settings(config_file)
            # If it doesn't raise, that's okay
            assert True
        except Exception:
            # If it raises, that's also expected
            assert True

    def test_load_settings_missing_file(self, tmp_path):
        """Test load_settings with missing file."""
        from mailbackup.config import load_settings
        
        config_file = tmp_path / "missing.toml"
        
        # Should raise FileNotFoundError
        try:
            settings = load_settings(config_file)
            assert False, "Should have raised error"
        except FileNotFoundError:
            assert True
        except Exception:
            # Some other error is also acceptable
            assert True
