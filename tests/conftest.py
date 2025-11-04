#!/usr/bin/env python3
"""
Shared pytest fixtures and configuration for mailbackup tests.
"""

import sys
from pathlib import Path

import pytest

# Ensure parent directory is in path for package imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

# Import from the mailbackup package
from mailbackup import config, db

Settings = config.Settings
ensure_schema = db.ensure_schema


@pytest.fixture
def tmp_dir(tmp_path):
    """Returns a temporary directory Path."""
    return tmp_path


@pytest.fixture
def sample_maildir(tmp_path):
    """Create a sample maildir structure for testing."""
    maildir = tmp_path / "maildir"
    maildir.mkdir()

    # Create some sample folders
    inbox = maildir / "INBOX" / "cur"
    inbox.mkdir(parents=True)

    sent = maildir / "Sent" / "cur"
    sent.mkdir(parents=True)

    spam = maildir / "Spam" / "cur"
    spam.mkdir(parents=True)

    return maildir


@pytest.fixture
def sample_email():
    """Return a simple RFC-822 compliant email message."""
    return b"""From: test@example.com
To: recipient@example.com
Subject: Test Email
Date: Mon, 1 Jan 2024 12:00:00 +0000
Message-ID: <test123@example.com>
Content-Type: text/plain; charset="utf-8"

This is a test email body.
"""


@pytest.fixture
def sample_email_with_attachment():
    """Return an email with an attachment."""
    return b"""From: sender@example.com
To: recipient@example.com
Subject: Email with Attachment
Date: Tue, 2 Jan 2024 14:30:00 +0000
Message-ID: <attach123@example.com>
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

This email has an attachment.

--boundary123
Content-Type: application/pdf; name="document.pdf"
Content-Disposition: attachment; filename="document.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQKJeLjz9MKMSAwIG9iago8PAovVHlwZSAvQ2F0YWxvZwovUGFnZXMgMiAwIFIKPj4K

--boundary123--
"""


@pytest.fixture
def test_db(tmp_path):
    """Create a test database with schema initialized."""
    db_path = tmp_path / "test.db"
    ensure_schema(db_path)
    return db_path


@pytest.fixture
def test_settings(tmp_path):
    """Create a Settings object with test paths."""
    return Settings(
        maildir=tmp_path / "maildir",
        attachments_dir=tmp_path / "attachments",
        remote="test-remote:Backups/Email",
        db_path=tmp_path / "state.db",
        log_path=tmp_path / "test.log",
        tmp_dir=tmp_path / "staging",
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
        status_interval=10,
        fetch_command="echo 'test fetch'",
        rclone_log_level="INFO",
        rclone_transfers=4,
        rclone_multi_thread_streams=2,
    )


@pytest.fixture
def mock_rclone(mocker):
    """Mock rclone command calls."""
    mock_run = mocker.patch("mailbackup.utils.run_cmd")
    mock_run.return_value = mocker.Mock(
        returncode=0,
        stdout="",
        stderr=""
    )
    return mock_run
