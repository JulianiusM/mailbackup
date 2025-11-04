#!/usr/bin/env python3
"""
Unit tests for orchestrator.py module.
"""

import subprocess
from unittest.mock import Mock

import pytest

from mailbackup.manifest import ManifestManager
from mailbackup.orchestrator import run_pipeline, _parse_command
from mailbackup.statistics import create_stats


class TestParseCommand:
    """Tests for _parse_command function."""

    def test_parse_simple_command(self):
        """Test parsing a simple command."""
        result = _parse_command("echo hello")
        assert result == ["echo", "hello"]

    def test_parse_command_with_quotes(self):
        """Test parsing command with quoted arguments."""
        result = _parse_command('echo "hello world"')
        assert result == ["echo", "hello world"]

    def test_parse_command_with_options(self):
        """Test parsing command with options."""
        result = _parse_command("git --no-pager log")
        assert result == ["git", "--no-pager", "log"]

    def test_parse_empty_command(self):
        """Test parsing empty command."""
        result = _parse_command("")
        assert result == []


class TestRunPipeline:
    """Tests for run_pipeline function."""

    @pytest.fixture
    def setup_mocks(self, mocker):
        """Set up common mocks for pipeline tests."""
        mocker.patch("mailbackup.orchestrator.run_streaming")
        mocker.patch("mailbackup.orchestrator.run_extractor")
        mocker.patch("mailbackup.orchestrator.incremental_upload")
        mocker.patch("mailbackup.orchestrator.rotate_archives")
        mocker.patch("mailbackup.orchestrator.integrity_check")
        # Mock logger to avoid 'status' attribute error
        mock_logger = mocker.MagicMock()
        mocker.patch("mailbackup.orchestrator.get_logger", return_value=mock_logger)
        mocker.patch("mailbackup.statistics.get_logger", return_value=mock_logger)

    def test_run_pipeline_fetch_only(self, test_settings, mocker, setup_mocks):
        """Test pipeline with fetch only."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_run_streaming = mocker.patch("mailbackup.orchestrator.run_streaming")

        run_pipeline(test_settings, manifest, stats, fetch=True, process=False, stages=[])

        # Verify fetch was called
        mock_run_streaming.assert_called_once()
        args = mock_run_streaming.call_args[0]
        assert "Fetching mail" in args[0]

    def test_run_pipeline_process_only(self, test_settings, mocker, setup_mocks):
        """Test pipeline with process only."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_run_extractor = mocker.patch("mailbackup.orchestrator.run_extractor")

        run_pipeline(test_settings, manifest, stats, fetch=False, process=True, stages=[])

        # Verify process was called
        mock_run_extractor.assert_called_once_with(test_settings, stats)

    def test_run_pipeline_backup_stage(self, test_settings, mocker, setup_mocks):
        """Test pipeline with backup stage."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_upload = mocker.patch("mailbackup.orchestrator.incremental_upload")

        run_pipeline(test_settings, manifest, stats, fetch=False, process=False, stages=["backup"])

        # Verify backup was called
        mock_upload.assert_called_once_with(test_settings, manifest, stats)

    def test_run_pipeline_archive_stage(self, test_settings, mocker, setup_mocks):
        """Test pipeline with archive stage."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_rotate = mocker.patch("mailbackup.orchestrator.rotate_archives")

        run_pipeline(test_settings, manifest, stats, fetch=False, process=False, stages=["archive"])

        # Verify archive was called
        mock_rotate.assert_called_once_with(test_settings, manifest, stats)

    def test_run_pipeline_check_stage(self, test_settings, mocker, setup_mocks):
        """Test pipeline with check stage."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_check = mocker.patch("mailbackup.orchestrator.integrity_check")

        run_pipeline(test_settings, manifest, stats, fetch=False, process=False, stages=["check"])

        # Verify check was called
        mock_check.assert_called_once_with(test_settings, manifest, stats)

    def test_run_pipeline_multiple_stages(self, test_settings, mocker, setup_mocks):
        """Test pipeline with multiple stages."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_upload = mocker.patch("mailbackup.orchestrator.incremental_upload")
        mock_rotate = mocker.patch("mailbackup.orchestrator.rotate_archives")
        mock_check = mocker.patch("mailbackup.orchestrator.integrity_check")

        run_pipeline(test_settings, manifest, stats, fetch=False, process=False,
                     stages=["backup", "archive", "check"])

        # Verify all stages were called in order
        mock_upload.assert_called_once()
        mock_rotate.assert_called_once()
        mock_check.assert_called_once()

    def test_run_pipeline_unknown_stage(self, test_settings, mocker, setup_mocks):
        """Test pipeline with unknown stage logs warning."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Should not raise exception
        run_pipeline(test_settings, manifest, stats, fetch=False, process=False,
                     stages=["unknown_stage"])

    def test_run_pipeline_fetch_no_command(self, test_settings, mocker, setup_mocks):
        """Test pipeline fetch without fetch_command raises error."""
        test_settings.fetch_command = None
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        with pytest.raises(RuntimeError, match="fetch was requested but no fetch_command"):
            run_pipeline(test_settings, manifest, stats, fetch=True, process=False, stages=[])

    def test_run_pipeline_fetch_command_failure(self, test_settings, mocker, setup_mocks):
        """Test pipeline handles fetch command failure."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_run_streaming = mocker.patch("mailbackup.orchestrator.run_streaming")
        mock_run_streaming.side_effect = subprocess.CalledProcessError(1, "mbsync")

        with pytest.raises(subprocess.CalledProcessError):
            run_pipeline(test_settings, manifest, stats, fetch=True, process=False, stages=[])

    def test_run_pipeline_full_workflow(self, test_settings, mocker, setup_mocks):
        """Test complete workflow: fetch + process + all stages."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_run_streaming = mocker.patch("mailbackup.orchestrator.run_streaming")
        mock_run_extractor = mocker.patch("mailbackup.orchestrator.run_extractor")
        mock_upload = mocker.patch("mailbackup.orchestrator.incremental_upload")
        mock_rotate = mocker.patch("mailbackup.orchestrator.rotate_archives")
        mock_check = mocker.patch("mailbackup.orchestrator.integrity_check")

        run_pipeline(test_settings, manifest, stats, fetch=True, process=True,
                     stages=["backup", "archive", "check"])

        # Verify all operations were called
        mock_run_streaming.assert_called_once()
        mock_run_extractor.assert_called_once()
        mock_upload.assert_called_once()
        mock_rotate.assert_called_once()
        mock_check.assert_called_once()

    def test_run_pipeline_process_exception_propagates(self, test_settings, mocker, setup_mocks):
        """Test that exceptions in process stage propagate."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        mock_run_extractor = mocker.patch("mailbackup.orchestrator.run_extractor")
        mock_run_extractor.side_effect = Exception("Process failed")

        with pytest.raises(Exception, match="Process failed"):
            run_pipeline(test_settings, manifest, stats, fetch=False, process=True, stages=[])

    def test_run_pipeline_none_stages(self, test_settings, mocker, setup_mocks):
        """Test that None stages defaults to empty list."""
        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Should not raise exception
        run_pipeline(test_settings, manifest, stats, fetch=False, process=False, stages=None)
