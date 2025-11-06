#!/usr/bin/env python3
"""
Unit tests for logger.py module.
"""

import logging

from mailbackup.logger import setup_logger, get_logger, STATUS_LEVEL
from mailbackup.config import Settings


class TestSetupLogger:
    """Tests for setup_logger function."""

    def test_setup_logger_creates_logger(self, tmp_path):
        """Test that setup_logger creates a logger with handlers."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger = setup_logger(settings)

            assert logger is not None
            assert logger.name == "mailbackup"
            assert len(logger.handlers) == 2  # file and console
            assert settings.log_path.exists()
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_setup_logger_sets_level(self, tmp_path):
        """Test that setup_logger sets the correct logging level."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="CRITICAL",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger = setup_logger(settings)

            assert logger.level == logging.CRITICAL
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_setup_logger_adds_status_level(self, tmp_path):
        """Test that setup_logger adds the STATUS level."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger = setup_logger(settings)

            assert logging.getLevelName(STATUS_LEVEL) == "STATUS"
            assert hasattr(logger, 'status')
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_setup_logger_idempotent(self, tmp_path):
        """Test that setup_logger can be called multiple times."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger1 = setup_logger(settings)
            logger2 = setup_logger(settings)

            assert logger1 is logger2
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_setup_logger_file_handler(self, tmp_path):
        """Test that setup_logger creates a file handler."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger = setup_logger(settings)

            file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) == 1
            assert file_handlers[0].level == logging.DEBUG
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_setup_logger_console_handler(self, tmp_path):
        """Test that setup_logger creates a console handler."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger = setup_logger(settings)

            console_handlers = [h for h in logger.handlers if
                                isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)]
            assert len(console_handlers) == 1
            assert console_handlers[0].level == logging.INFO
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_setup_logger_no_propagation(self, tmp_path):
        """Test that setup_logger disables propagation."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger = setup_logger(settings)

            assert logger.propagate is False
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_status_method_exists(self, tmp_path):
        """Test that status method is added to Logger."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            logger = setup_logger(settings)

            assert hasattr(logging.Logger, 'status')
            # Test that the method is callable
            logger.status("Test status message")
        finally:
            mailbackup.logger._LOGGER = old_logger


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_after_setup(self, tmp_path):
        """Test get_logger returns child logger after setup."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            setup_logger(settings)
            logger = get_logger("test_module")

            assert logger.name == "mailbackup.test_module"
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_get_logger_no_name(self, tmp_path):
        """Test get_logger without name returns root logger."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            settings = Settings(
                maildir=tmp_path / "maildir",
                attachments_dir=tmp_path / "attachments",
                remote="test:remote",
                db_path=tmp_path / "test.db",
                log_path=tmp_path / "test.log",
                tmp_dir=tmp_path / "tmp",
                archive_dir=tmp_path / "archives",
                manifest_path=tmp_path / "manifest.csv",
                retention_years=2,
                keep_local_after_archive=False,
                verify_integrity=True,
                repair_on_failure=True,
                manifest_remote_name="manifest.csv",
                max_manifest_conflict_retries=3,
                max_hash_threads=2,
                max_upload_workers=2,
                max_extract_workers=1,
                upload_batch_size=100,
                status_interval=10,
                log_level="INFO",
                rotate_by_time=True,
                max_log_files=10,
                max_log_size=50 * 1024 * 1024,
                fetch_command="",
                rclone_log_level="INFO",
                rclone_transfers=4,
                rclone_multi_thread_streams=2,
            )
            setup_logger(settings)
            logger = get_logger()

            assert logger.name == "mailbackup"
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_get_logger_before_setup(self):
        """Test get_logger returns fallback logger before setup."""
        # Reset global logger
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            logger = get_logger("test_module")

            # Should return a fallback logger
            assert logger is not None
            assert "mailbackup.temp" in logger.name
        finally:
            mailbackup.logger._LOGGER = old_logger

