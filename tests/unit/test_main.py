#!/usr/bin/env python3
"""
Unit tests for __main__.py module.
"""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from argparse import Namespace
from mailbackup.__main__ import build_parser, main


class TestBuildParser:
    """Tests for build_parser function."""
    
    def test_build_parser_creates_parser(self):
        """Test that build_parser creates an ArgumentParser."""
        parser = build_parser()
        assert parser is not None
    
    def test_parser_accepts_fetch_action(self):
        """Test that parser accepts 'fetch' action."""
        parser = build_parser()
        args = parser.parse_args(["fetch"])
        assert args.action == "fetch"
    
    def test_parser_accepts_process_action(self):
        """Test that parser accepts 'process' action."""
        parser = build_parser()
        args = parser.parse_args(["process"])
        assert args.action == "process"
    
    def test_parser_accepts_backup_action(self):
        """Test that parser accepts 'backup' action."""
        parser = build_parser()
        args = parser.parse_args(["backup"])
        assert args.action == "backup"
    
    def test_parser_accepts_archive_action(self):
        """Test that parser accepts 'archive' action."""
        parser = build_parser()
        args = parser.parse_args(["archive"])
        assert args.action == "archive"
    
    def test_parser_accepts_check_action(self):
        """Test that parser accepts 'check' action."""
        parser = build_parser()
        args = parser.parse_args(["check"])
        assert args.action == "check"
    
    def test_parser_accepts_run_action(self):
        """Test that parser accepts 'run' action."""
        parser = build_parser()
        args = parser.parse_args(["run"])
        assert args.action == "run"
    
    def test_parser_accepts_full_action(self):
        """Test that parser accepts 'full' action."""
        parser = build_parser()
        args = parser.parse_args(["full"])
        assert args.action == "full"
    
    def test_parser_accepts_legacy_extract(self):
        """Test that parser accepts legacy 'extract' action."""
        parser = build_parser()
        args = parser.parse_args(["extract"])
        assert args.action == "extract"
    
    def test_parser_accepts_legacy_upload(self):
        """Test that parser accepts legacy 'upload' action."""
        parser = build_parser()
        args = parser.parse_args(["upload"])
        assert args.action == "upload"
    
    def test_parser_accepts_legacy_rotate(self):
        """Test that parser accepts legacy 'rotate' action."""
        parser = build_parser()
        args = parser.parse_args(["rotate"])
        assert args.action == "rotate"
    
    def test_parser_accepts_legacy_verify(self):
        """Test that parser accepts legacy 'verify' action."""
        parser = build_parser()
        args = parser.parse_args(["verify"])
        assert args.action == "verify"
    
    def test_parser_accepts_config_option(self):
        """Test that parser accepts --config option."""
        parser = build_parser()
        args = parser.parse_args(["fetch", "--config", "/path/to/config.toml"])
        assert args.config == Path("/path/to/config.toml")
    
    def test_parser_config_default_none(self):
        """Test that config defaults to None."""
        parser = build_parser()
        args = parser.parse_args(["fetch"])
        assert args.config is None


class TestMain:
    """Tests for main function."""
    
    @pytest.fixture
    def mock_dependencies(self, mocker):
        """Mock all dependencies for main function."""
        mocker.patch("mailbackup.__main__.load_settings")
        mocker.patch("mailbackup.__main__.setup_logger")
        mocker.patch("mailbackup.__main__.get_logger")
        mocker.patch("mailbackup.__main__.ensure_dirs")
        mocker.patch("mailbackup.__main__.db.ensure_schema")
        mocker.patch("mailbackup.__main__.ManifestManager")
        mocker.patch("mailbackup.__main__.StatusThread")
        mocker.patch("mailbackup.__main__.install_signal_handlers")
        mocker.patch("mailbackup.__main__.run_pipeline")
        
        # Return mocks
        return {
            "load_settings": mocker.patch("mailbackup.__main__.load_settings"),
            "setup_logger": mocker.patch("mailbackup.__main__.setup_logger"),
            "get_logger": mocker.patch("mailbackup.__main__.get_logger"),
            "ensure_dirs": mocker.patch("mailbackup.__main__.ensure_dirs"),
            "ensure_schema": mocker.patch("mailbackup.__main__.db.ensure_schema"),
            "ManifestManager": mocker.patch("mailbackup.__main__.ManifestManager"),
            "StatusThread": mocker.patch("mailbackup.__main__.StatusThread"),
            "install_signal_handlers": mocker.patch("mailbackup.__main__.install_signal_handlers"),
            "run_pipeline": mocker.patch("mailbackup.__main__.run_pipeline"),
        }
    
    def test_main_fetch_action(self, mocker, test_settings, mock_dependencies):
        """Test main with fetch action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert call_kwargs["fetch"] is True
        assert call_kwargs["process"] is False
    
    def test_main_process_action(self, mocker, test_settings, mock_dependencies):
        """Test main with process action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "process"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert call_kwargs["fetch"] is False
        assert call_kwargs["process"] is True
    
    def test_main_backup_action(self, mocker, test_settings, mock_dependencies):
        """Test main with backup action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "backup"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert "backup" in call_kwargs["stages"]
    
    def test_main_archive_action(self, mocker, test_settings, mock_dependencies):
        """Test main with archive action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "archive"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert "archive" in call_kwargs["stages"]
    
    def test_main_check_action(self, mocker, test_settings, mock_dependencies):
        """Test main with check action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "check"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert "check" in call_kwargs["stages"]
    
    def test_main_run_action(self, mocker, test_settings, mock_dependencies):
        """Test main with run action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "run"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert call_kwargs["process"] is True
        assert "backup" in call_kwargs["stages"]
        assert "archive" in call_kwargs["stages"]
        assert "check" in call_kwargs["stages"]
    
    def test_main_full_action(self, mocker, test_settings, mock_dependencies):
        """Test main with full action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "full"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert call_kwargs["fetch"] is True
        assert call_kwargs["process"] is True
        assert "backup" in call_kwargs["stages"]
        assert "archive" in call_kwargs["stages"]
        assert "check" in call_kwargs["stages"]
    
    def test_main_legacy_extract(self, mocker, test_settings, mock_dependencies):
        """Test main with legacy extract action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "extract"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert call_kwargs["process"] is True
    
    def test_main_legacy_upload(self, mocker, test_settings, mock_dependencies):
        """Test main with legacy upload action."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "upload"])
        
        main()
        
        mock_dependencies["run_pipeline"].assert_called_once()
        call_kwargs = mock_dependencies["run_pipeline"].call_args[1]
        assert "backup" in call_kwargs["stages"]
    
    def test_main_with_config(self, mocker, test_settings, mock_dependencies):
        """Test main with --config option."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "fetch", "--config", "/path/to/config.toml"])
        
        main()
        
        mock_dependencies["load_settings"].assert_called_once()
        assert mock_dependencies["load_settings"].call_args[0][0] == Path("/path/to/config.toml")
    
    def test_main_ensures_dirs(self, mocker, test_settings, mock_dependencies):
        """Test main ensures required directories exist."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        main()
        
        mock_dependencies["ensure_dirs"].assert_called_once()
    
    def test_main_ensures_db_schema(self, mocker, test_settings, mock_dependencies):
        """Test main ensures database schema."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        main()
        
        mock_dependencies["ensure_schema"].assert_called_once_with(test_settings.db_path)
    
    def test_main_db_schema_failure_exits(self, mocker, test_settings, mock_dependencies):
        """Test main exits when database schema creation fails."""
        mock_dependencies["load_settings"].return_value = test_settings
        mock_dependencies["ensure_schema"].side_effect = Exception("DB error")
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 2
    
    def test_main_creates_manifest_manager(self, mocker, test_settings, mock_dependencies):
        """Test main creates ManifestManager."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        main()
        
        mock_dependencies["ManifestManager"].assert_called_once_with(test_settings)
    
    def test_main_starts_status_thread(self, mocker, test_settings, mock_dependencies):
        """Test main starts StatusThread."""
        mock_dependencies["load_settings"].return_value = test_settings
        mock_thread = MagicMock()
        mock_dependencies["StatusThread"].return_value = mock_thread
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        main()
        
        mock_thread.start.assert_called_once()
        mock_thread.stop.assert_called_once()
    
    def test_main_installs_signal_handlers(self, mocker, test_settings, mock_dependencies):
        """Test main installs signal handlers."""
        mock_dependencies["load_settings"].return_value = test_settings
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        main()
        
        mock_dependencies["install_signal_handlers"].assert_called_once()
    
    def test_main_dumps_manifest_on_completion(self, mocker, test_settings, mock_dependencies):
        """Test main dumps manifest queue on completion."""
        mock_dependencies["load_settings"].return_value = test_settings
        mock_manifest = MagicMock()
        mock_dependencies["ManifestManager"].return_value = mock_manifest
        mocker.patch("sys.argv", ["mailbackup", "fetch"])
        
        main()
        
        # dump_queue should be called at least once (in finally block)
        assert mock_manifest.dump_queue.call_count >= 1
