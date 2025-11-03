#!/usr/bin/env python3
"""
Unit tests for rclone.py module.
"""

import pytest
from pathlib import Path
from mailbackup.rclone import (
    set_rclone_defaults,
    rclone_copy,
    rclone_copyto,
    rclone_moveto,
    rclone_cat,
    rclone_hashsum,
    rclone_deletefile,
    rclone_lsjson,
    rclone_lsf,
    rclone_check,
    RCLONE_BASE,
)


class TestSetRcloneDefaults:
    """Tests for set_rclone_defaults function."""
    
    def test_set_rclone_defaults(self):
        set_rclone_defaults(log_level="DEBUG", transfers=8, multi_thread_streams=4)
        
        # RCLONE_BASE should be updated
        from mailbackup import rclone
        assert "--log-level=DEBUG" in rclone.RCLONE_BASE
        assert "--transfers=8" in rclone.RCLONE_BASE
        assert "--multi-thread-streams=4" in rclone.RCLONE_BASE
    
    def test_set_rclone_defaults_default_values(self):
        set_rclone_defaults()
        
        from mailbackup import rclone
        # Should have some defaults
        assert "rclone" in rclone.RCLONE_BASE


class TestRcloneCopy:
    """Tests for rclone_copy function."""
    
    def test_rclone_copy_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(returncode=0, stdout="", stderr="")
        
        rclone_copy("/local/path", "remote:/path")
        
        # Verify _run_rclone was called
        mock_run.assert_called_once()
        args = mock_run.call_args[0]
        
        assert "copy" in args
        assert "/local/path" in args
        assert "remote:/path" in args
    
    def test_rclone_copy_with_extra_args(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(returncode=0, stdout="", stderr="")
        
        rclone_copy("/local", "remote:/", "--dry-run", "--verbose")
        
        args = mock_run.call_args[0]
        assert "--dry-run" in args
        assert "--verbose" in args


class TestRcloneCopyto:
    """Tests for rclone_copyto function."""
    
    def test_rclone_copyto_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(returncode=0, stdout="", stderr="")
        
        rclone_copyto("/local/file.txt", "remote:/path/file.txt")
        
        args = mock_run.call_args[0]
        assert "copyto" in args
        assert "/local/file.txt" in args
        assert "remote:/path/file.txt" in args


class TestRcloneMoveto:
    """Tests for rclone_moveto function."""
    
    def test_rclone_moveto_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(returncode=0, stdout="", stderr="")
        
        rclone_moveto("remote:/old.txt", "remote:/new.txt")
        
        args = mock_run.call_args[0]
        assert "moveto" in args
        assert "remote:/old.txt" in args
        assert "remote:/new.txt" in args


class TestRcloneCat:
    """Tests for rclone_cat function."""
    
    def test_rclone_cat_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(
            returncode=0,
            stdout="file contents",
            stderr=""
        )
        
        result = rclone_cat("remote:/path/file.txt")
        
        args = mock_run.call_args[0]
        assert "cat" in args
        assert "remote:/path/file.txt" in args
        assert result.stdout == "file contents"


class TestRcloneHashsum:
    """Tests for rclone_hashsum function."""
    
    def test_rclone_hashsum_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(
            returncode=0,
            stdout="abc123 file.txt\n",
            stderr=""
        )
        
        result = rclone_hashsum("SHA256", "remote:/path")
        
        args = mock_run.call_args[0]
        assert "hashsum" in args
        assert "SHA256" in args
        assert "remote:/path" in args


class TestRcloneDeletefile:
    """Tests for rclone_deletefile function."""
    
    def test_rclone_deletefile_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(returncode=0, stdout="", stderr="")
        
        rclone_deletefile("remote:/path/file.txt")
        
        args = mock_run.call_args[0]
        assert "deletefile" in args
        assert "remote:/path/file.txt" in args


class TestRcloneLsjson:
    """Tests for rclone_lsjson function."""
    
    def test_rclone_lsjson_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(
            returncode=0,
            stdout='[{"Path": "file.txt", "Size": 100}]',
            stderr=""
        )
        
        result = rclone_lsjson("remote:/path")
        
        args = mock_run.call_args[0]
        assert "lsjson" in args
        assert "remote:/path" in args
        assert "file.txt" in result.stdout


class TestRcloneLsf:
    """Tests for rclone_lsf function."""
    
    def test_rclone_lsf_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(
            returncode=0,
            stdout="file1.txt\nfile2.txt\n",
            stderr=""
        )
        
        result = rclone_lsf("remote:/path")
        
        args = mock_run.call_args[0]
        assert "lsf" in args
        assert "remote:/path" in args


class TestRcloneCheck:
    """Tests for rclone_check function."""
    
    def test_rclone_check_command(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(returncode=0, stdout="", stderr="")
        
        rclone_check("remote1:/path", "remote2:/path")
        
        args = mock_run.call_args[0]
        assert "check" in args
        assert "remote1:/path" in args
        assert "remote2:/path" in args


class TestRcloneCheckFalse:
    """Tests for rclone commands with check=False."""
    
    def test_rclone_copy_check_false(self, mocker):
        mock_run = mocker.patch("mailbackup.rclone._run_rclone")
        mock_run.return_value = mocker.Mock(returncode=1, stdout="", stderr="error")
        
        # Should not raise even with non-zero return code
        result = rclone_copy("/local", "remote:/", check=False)
        
        # Just verify it was called
        mock_run.assert_called_once()
        kwargs = mock_run.call_args[1]
        assert kwargs.get("check") is False
