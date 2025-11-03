#!/usr/bin/env python3
"""
Unit tests for logger.py module.
"""

import logging

from mailbackup.logger import setup_logger, get_logger, STATUS_LEVEL


class TestSetupLogger:
    """Tests for setup_logger function."""

    def test_setup_logger_creates_logger(self, tmp_path):
        """Test that setup_logger creates a logger with handlers."""
        log_path = tmp_path / "test.log"
        logger = setup_logger(log_path)

        assert logger is not None
        assert logger.name == "mailbackup"
        assert len(logger.handlers) == 2  # file and console
        assert log_path.exists()

    def test_setup_logger_sets_level(self, tmp_path):
        """Test that setup_logger sets the correct logging level."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            log_path = tmp_path / "test.log"
            logger = setup_logger(log_path, level=logging.CRITICAL)

            assert logger.level == logging.CRITICAL
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_setup_logger_adds_status_level(self, tmp_path):
        """Test that setup_logger adds the STATUS level."""
        log_path = tmp_path / "test.log"
        logger = setup_logger(log_path)

        assert logging.getLevelName(STATUS_LEVEL) == "STATUS"
        assert hasattr(logger, 'status')

    def test_setup_logger_idempotent(self, tmp_path):
        """Test that setup_logger can be called multiple times."""
        log_path = tmp_path / "test.log"
        logger1 = setup_logger(log_path)
        logger2 = setup_logger(log_path)

        assert logger1 is logger2

    def test_setup_logger_file_handler(self, tmp_path):
        """Test that setup_logger creates a file handler."""
        log_path = tmp_path / "test.log"
        logger = setup_logger(log_path)

        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        assert file_handlers[0].level == logging.DEBUG

    def test_setup_logger_console_handler(self, tmp_path):
        """Test that setup_logger creates a console handler."""
        log_path = tmp_path / "test.log"
        logger = setup_logger(log_path)

        console_handlers = [h for h in logger.handlers if
                            isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == logging.INFO

    def test_setup_logger_no_propagation(self, tmp_path):
        """Test that setup_logger disables propagation."""
        log_path = tmp_path / "test.log"
        logger = setup_logger(log_path)

        assert logger.propagate is False

    def test_status_method_exists(self, tmp_path):
        """Test that the status method is added to logger."""
        # Reset global logger to test fresh initialization
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            log_path = tmp_path / "test.log"
            logger = setup_logger(log_path)

            # Should not raise AttributeError
            logger.status("Test status message")

            # Verify log file contains the message
            assert log_path.exists()
            with open(log_path, 'r') as f:
                content = f.read()
                assert "Test status message" in content
        finally:
            mailbackup.logger._LOGGER = old_logger


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_without_setup(self):
        """Test that get_logger works before setup_logger is called."""
        # Reset global logger
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            logger = get_logger("test_module")
            assert logger is not None
            assert "test_module" in logger.name
        finally:
            mailbackup.logger._LOGGER = old_logger

    def test_get_logger_after_setup(self, tmp_path):
        """Test that get_logger returns child logger after setup."""
        log_path = tmp_path / "test.log"
        setup_logger(log_path)

        logger = get_logger("test_module")
        assert logger is not None
        assert "test_module" in logger.name

    def test_get_logger_no_name(self, tmp_path):
        """Test that get_logger works without a name parameter."""
        log_path = tmp_path / "test.log"
        setup_logger(log_path)

        logger = get_logger()
        assert logger is not None
        assert logger.name == "mailbackup"

    def test_get_logger_fallback_creates_stderr_logger(self):
        """Test that fallback logger logs to stderr."""
        import mailbackup.logger
        old_logger = mailbackup.logger._LOGGER
        mailbackup.logger._LOGGER = None

        try:
            logger = get_logger()
            assert logger is not None
            # The fallback logger should have at least one handler
            temp_logger = logging.getLogger("mailbackup.temp")
            assert len(temp_logger.handlers) > 0
        finally:
            mailbackup.logger._LOGGER = old_logger
