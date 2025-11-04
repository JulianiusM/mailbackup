#!/usr/bin/env python3
"""
Integration tests for integrity.py module.

Tests the integrity checking, verification, and repair functionality
with mocked rclone commands and actual database/filesystem operations.
"""

import json
from mailbackup.statistics import StatKey, create_stats
import shutil
from unittest.mock import Mock

import pytest
from mailbackup.statistics import StatKey, create_stats

from mailbackup.integrity import integrity_check, repair_remote, rebuild_docset
from mailbackup.manifest import ManifestManager


@pytest.mark.integration
class TestIntegrityCheckIntegration:
    """Integration tests for integrity_check function."""

    def test_integrity_check_happy_path_all_match(self, test_settings, mocker):
        """Test successful integrity check where all remote files match local database."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        manifest_file.write_text("abc123hash,2024/folder/email.eml\ndef456hash,2024/folder2/email.eml\n")

        # Mock rclone copyto to copy manifest
        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        # Mock DB with matching data
        db_rows = [
            {
                "id": 1,
                "hash": "abc123",
                "hash_sha256": "abc123hash",
                "path": "/path/to/email1.eml",
                "remote_path": "2024/folder/email.eml",
                "from_header": "test@example.com",
                "subject": "Test 1",
                "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
                "attachments": "[]",
                "spam": 0,
                "processed_at": "2024-01-01 12:00:00",
            },
            {
                "id": 2,
                "hash": "def456",
                "hash_sha256": "def456hash",
                "path": "/path/to/email2.eml",
                "remote_path": "2024/folder2/email.eml",
                "from_header": "test2@example.com",
                "subject": "Test 2",
                "date_header": "Mon, 2 Jan 2024 12:00:00 +0000",
                "attachments": "[]",
                "spam": 0,
                "processed_at": "2024-01-02 12:00:00",
            },
        ]
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=db_rows)

        manifest = Mock(spec=ManifestManager)
        manifest.manifest_path = test_settings.manifest_path
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert
        assert stats[StatKey.VERIFIED] == 2
        assert stats.get("repaired", 0) == 0
        manifest.upload_manifest_if_needed.assert_called_once()

    def test_integrity_check_with_rclone_hashsum_fallback(self, test_settings, mocker):
        """Test integrity check using rclone hashsum when manifest is missing."""
        # Mock rclone copyto to not create file (manifest missing)
        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        # Mock remote_hash to return hash map
        remote_hashes = {
            "2024/folder/email.eml": "abc123hash"
        }
        mocker.patch("mailbackup.integrity.remote_hash", return_value=remote_hashes)

        # Mock DB data
        db_row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "abc123hash",
            "path": "/path/to/email.eml",
            "remote_path": "2024/folder/email.eml",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert
        assert stats[StatKey.VERIFIED] == 1
        assert stats.get("repaired", 0) == 0

    def test_integrity_check_disabled(self, test_settings, mocker):
        """Test that integrity check is skipped when disabled in settings."""
        test_settings.verify_integrity = False

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert - should exit early
        assert stats[StatKey.VERIFIED] == 0

    def test_integrity_check_missing_remote_file(self, test_settings, mocker):
        """Test integrity check detects missing remote files."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        # Only one file in manifest
        manifest_file.write_text("abc123hash,2024/folder/email.eml\n")

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        # DB has two files, but manifest only has one
        db_rows = [
            {
                "id": 1,
                "hash": "abc123",
                "hash_sha256": "abc123hash",
                "path": "/path/to/email1.eml",
                "remote_path": "2024/folder/email.eml",
                "from_header": "test@example.com",
                "subject": "Test 1",
                "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
                "attachments": "[]",
                "spam": 0,
                "processed_at": "2024-01-01 12:00:00",
            },
            {
                "id": 2,
                "hash": "def456",
                "hash_sha256": "def456hash",
                "path": "/path/to/email2.eml",
                "remote_path": "2024/folder2/email.eml",  # Missing from manifest
                "from_header": "test2@example.com",
                "subject": "Test 2",
                "date_header": "Mon, 2 Jan 2024 12:00:00 +0000",
                "attachments": "[]",
                "spam": 0,
                "processed_at": "2024-01-02 12:00:00",
            },
        ]
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=db_rows)

        # Disable repair for this test
        test_settings.repair_on_failure = False

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert - should verify both but not repair
        assert stats[StatKey.VERIFIED] == 2
        assert stats.get("repaired", 0) == 0

    def test_integrity_check_hash_mismatch(self, test_settings, mocker):
        """Test integrity check detects hash mismatches."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        # Hash in manifest doesn't match DB
        manifest_file.write_text("wronghash123,2024/folder/email.eml\n")

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        # DB has different hash
        db_row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "correcthash456",  # Mismatch!
            "path": "/path/to/email.eml",
            "remote_path": "2024/folder/email.eml",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])

        # Disable repair for this test
        test_settings.repair_on_failure = False

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert
        assert stats[StatKey.VERIFIED] == 1
        assert stats.get("repaired", 0) == 0

    def test_integrity_check_with_repair_missing_file(self, test_settings, mocker):
        """Test integrity check repairs missing remote files."""
        # Setup real filesystem for repair
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        manifest_file = test_settings.tmp_dir / "manifest.csv"
        # Empty manifest - file is missing
        manifest_file.write_text("")

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        # DB row for missing file
        db_row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "correcthash456",
            "path": str(email_path),
            "remote_path": "2024/folder/email.eml",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])

        # Mock DB update
        mock_update_path = mocker.patch("mailbackup.integrity.db.update_remote_path")

        # Mock atomic upload to succeed
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value=True)

        # Enable repair
        test_settings.repair_on_failure = True

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert
        assert stats[StatKey.VERIFIED] == 1
        assert stats[StatKey.REPAIRED] == 1 
        manifest.queue_entry.assert_called_once()
        mock_update_path.assert_called_once()

    def test_integrity_check_with_repair_hash_mismatch(self, test_settings, mocker):
        """Test integrity check repairs hash mismatches."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        manifest_file = test_settings.tmp_dir / "manifest.csv"
        # Wrong hash
        manifest_file.write_text("wronghash,2024/folder/email.eml\n")

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        db_row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "correcthash",
            "path": str(email_path),
            "remote_path": "2024/folder/email.eml",
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])

        # Mock DB update
        mocker.patch("mailbackup.integrity.db.update_remote_path")

        # Mock atomic upload to succeed
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value=True)

        # Enable repair
        test_settings.repair_on_failure = True

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert
        assert stats[StatKey.VERIFIED] == 1
        assert stats[StatKey.REPAIRED] == 1 

    def test_integrity_check_no_remote_hashsum_available(self, test_settings, mocker):
        """Test integrity check when both manifest and hashsum are unavailable."""
        # Mock rclone copyto to not create file
        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        # Mock remote_hash to return None
        mocker.patch("mailbackup.integrity.remote_hash", return_value=None)

        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[])

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert - should exit early
        assert stats[StatKey.VERIFIED] == 0
        assert stats.get("repaired", 0) == 0

    def test_integrity_check_skips_empty_remote_path(self, test_settings, mocker):
        """Test that entries with no remote_path are skipped."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = test_settings.tmp_dir / "manifest.csv"
        manifest_file.write_text("abc123hash,2024/folder/email.eml\n")

        mocker.patch("mailbackup.integrity.rclone_copyto", return_value=Mock(returncode=0))

        # DB row with empty remote_path
        db_row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "abc123hash",
            "path": "/path/to/email.eml",
            "remote_path": "",  # Empty!
            "from_header": "test@example.com",
            "subject": "Test",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }
        mocker.patch("mailbackup.integrity.db.fetch_synced", return_value=[db_row])

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()

        # Execute
        integrity_check(test_settings, manifest, stats)

        # Assert - should not count as verified since remote_path is empty
        assert stats[StatKey.VERIFIED] == 0


@pytest.mark.integration
class TestRepairRemoteIntegration:
    """Integration tests for repair_remote function."""

    def test_repair_remote_happy_path(self, test_settings, mocker):
        """Test successful repair of a remote document set."""
        # Setup real filesystem
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        # DB row
        row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "hashvalue",
            "path": str(email_path),
            "remote_path": "2024/old/email.eml",
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        # Mock atomic upload to succeed
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value=True)

        # Mock DB update
        mock_update_path = mocker.patch("mailbackup.integrity.db.update_remote_path")

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()
        logger = Mock()

        # Execute
        repair_remote(test_settings, "missing", row, manifest, stats)

        # Assert
        assert stats[StatKey.REPAIRED] == 1 
        manifest.queue_entry.assert_called_once()
        mock_update_path.assert_called_once()

    def test_repair_remote_with_attachments(self, test_settings, mocker):
        """Test repair with email and attachments."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        test_settings.attachments_dir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        attach_path = test_settings.attachments_dir / "2024" / "test.pdf"
        attach_path.parent.mkdir(parents=True, exist_ok=True)
        attach_path.write_bytes(b"PDF content")

        row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "hashvalue",
            "path": str(email_path),
            "remote_path": "2024/old/email.eml",
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": json.dumps([str(attach_path)]),
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        # Mock atomic upload to succeed
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.integrity.db.update_remote_path")

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()
        logger = Mock()

        # Execute
        repair_remote(test_settings, "missing", row, manifest, stats)

        # Assert
        assert stats[StatKey.REPAIRED] == 1 

    def test_repair_remote_upload_failure(self, test_settings, mocker):
        """Test repair handles upload failures gracefully."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "hashvalue",
            "path": str(email_path),
            "remote_path": "2024/old/email.eml",
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        # Mock atomic upload to fail
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value=False)

        # Mock DB update - should not be called on failure
        mock_update_path = mocker.patch("mailbackup.integrity.db.update_remote_path")

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()
        logger = Mock()

        # Execute
        repair_remote(test_settings, "missing", row, manifest, stats)

        # Assert - repaired counter still increments (tracks attempts)
        assert stats[StatKey.FAILED] == 1  # Repair attempt failed
        # But DB should not be updated
        mock_update_path.assert_not_called()
        manifest.queue_entry.assert_not_called()

    def test_repair_remote_missing_local_email(self, test_settings, mocker):
        """Test repair when local email file is missing."""
        # Setup - don't create email file
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)

        row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "hashvalue",
            "path": "/nonexistent/email.eml",
            "remote_path": "2024/old/email.eml",
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": "[]",
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        # Mock atomic upload - will be called with info.json at minimum
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.integrity.db.update_remote_path")

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()
        logger = Mock()

        # Execute - should not crash
        repair_remote(test_settings, "missing", row, manifest, stats)

        # Assert - repair is attempted even without local file
        assert stats[StatKey.REPAIRED] == 1 

    def test_repair_remote_missing_local_attachments(self, test_settings, mocker):
        """Test repair when local attachments are missing."""
        # Setup
        test_settings.tmp_dir.mkdir(parents=True, exist_ok=True)
        test_settings.maildir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        # Reference non-existent attachment
        row = {
            "id": 1,
            "hash": "abc123",
            "hash_sha256": "hashvalue",
            "path": str(email_path),
            "remote_path": "2024/old/email.eml",
            "from_header": "test@example.com",
            "subject": "Test Email",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": json.dumps(["/nonexistent/attachment.pdf"]),
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        # Mock atomic upload
        mocker.patch("mailbackup.integrity.atomic_upload_file", return_value=True)
        mocker.patch("mailbackup.integrity.db.update_remote_path")

        manifest = Mock(spec=ManifestManager)
        stats = create_stats()
        logger = Mock()

        # Execute - should not crash
        repair_remote(test_settings, "missing", row, manifest, stats)

        # Assert
        assert stats[StatKey.REPAIRED] == 1 


@pytest.mark.integration
class TestRebuildDocsetIntegration:
    """Integration tests for rebuild_docset function."""

    def test_rebuild_docset_integration_scenario(self, test_settings):
        """Test rebuild_docset in realistic integration scenario."""
        # Setup
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        test_settings.attachments_dir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        attach_path = test_settings.attachments_dir / "2024" / "document.pdf"
        attach_path.parent.mkdir(parents=True, exist_ok=True)
        attach_path.write_bytes(b"PDF content here")

        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_path),
            "from_header": "sender@example.com",
            "subject": "Important Document",
            "date_header": "Mon, 1 Jan 2024 14:30:00 +0000",
            "attachments": json.dumps([str(attach_path)]),
            "spam": 0,
            "processed_at": "2024-01-01 14:30:00",
        }

        # Execute
        result = rebuild_docset(test_settings, 2024, "test_rebuild_folder", row)

        # Assert
        assert result.exists()
        assert result.is_dir()
        assert (result / "email.eml").exists()
        assert (result / "info.json").exists()

        # Check that attachment was copied
        pdf_files = list(result.glob("*.pdf"))
        assert len(pdf_files) == 1
        assert pdf_files[0].read_bytes() == b"PDF content here"

        # Verify info.json content
        import json as json_module
        info_data = json_module.loads((result / "info.json").read_text())
        assert info_data["id"] == 1
        assert info_data["hash"] == "abc123"
        assert "email.eml" in info_data["remote_path"]

        # Cleanup
        shutil.rmtree(result.parent.parent, ignore_errors=True)

    def test_rebuild_docset_multiple_attachments(self, test_settings):
        """Test rebuild with multiple attachments."""
        # Setup
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        test_settings.attachments_dir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        # Create multiple attachments
        attach_dir = test_settings.attachments_dir / "2024"
        attach_dir.mkdir(parents=True, exist_ok=True)

        attach1 = attach_dir / "doc1.pdf"
        attach1.write_bytes(b"PDF 1")

        attach2 = attach_dir / "doc2.txt"
        attach2.write_bytes(b"Text file")

        attach3 = attach_dir / "image.jpg"
        attach3.write_bytes(b"JPEG data")

        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_path),
            "from_header": "sender@example.com",
            "subject": "Multiple Attachments",
            "date_header": "Mon, 1 Jan 2024 14:30:00 +0000",
            "attachments": json.dumps([str(attach1), str(attach2), str(attach3)]),
            "spam": 0,
            "processed_at": "2024-01-01 14:30:00",
        }

        # Execute
        result = rebuild_docset(test_settings, 2024, "multi_attach", row)

        # Assert
        assert result.exists()
        assert (result / "email.eml").exists()
        assert (result / "info.json").exists()

        # Check all attachments
        assert len(list(result.glob("*.pdf"))) == 1
        assert len(list(result.glob("*.txt"))) == 1
        assert len(list(result.glob("*.jpg"))) == 1

        # Cleanup
        shutil.rmtree(result.parent.parent, ignore_errors=True)

    def test_rebuild_docset_special_characters_in_filename(self, test_settings):
        """Test rebuild handles special characters in filenames correctly."""
        # Setup
        test_settings.maildir.mkdir(parents=True, exist_ok=True)
        test_settings.attachments_dir.mkdir(parents=True, exist_ok=True)

        email_path = test_settings.maildir / "test.eml"
        email_path.write_text("From: test@example.com\nSubject: Test\n\nBody")

        # Attachment with special characters
        attach_path = test_settings.attachments_dir / "2024" / "file with spaces & special.pdf"
        attach_path.parent.mkdir(parents=True, exist_ok=True)
        attach_path.write_bytes(b"PDF content")

        row = {
            "id": 1,
            "hash": "abc123",
            "path": str(email_path),
            "from_header": "test@example.com",
            "subject": "Test: Special < > Characters?",
            "date_header": "Mon, 1 Jan 2024 12:00:00 +0000",
            "attachments": json.dumps([str(attach_path)]),
            "spam": 0,
            "processed_at": "2024-01-01 12:00:00",
        }

        # Execute - should sanitize names
        result = rebuild_docset(test_settings, 2024, "special_chars", row)

        # Assert
        assert result.exists()
        assert (result / "email.eml").exists()
        assert (result / "info.json").exists()

        # Attachment should be copied with sanitized name
        pdf_files = list(result.glob("*.pdf"))
        assert len(pdf_files) == 1
        # Sanitized filename should not contain special chars
        assert "<" not in pdf_files[0].name
        assert ">" not in pdf_files[0].name

        # Cleanup
        shutil.rmtree(result.parent.parent, ignore_errors=True)


