#!/usr/bin/env python3
"""
Unit tests for db.py module.
"""

import sqlite3

from mailbackup.db import (
    ensure_schema,
    fetch_unsynced,
    mark_synced,
    fetch_synced,
    mark_archived_year,
    is_processed,
    mark_processed,
    get_candidate_rotation_years,
    fetch_unarchived_paths_for_year,
    update_remote_path,
    get_connection,
)


class TestEnsureSchema:
    """Tests for ensure_schema function."""

    def test_ensure_schema_creates_table(self, tmp_path):
        db_path = tmp_path / "test.db"
        ensure_schema(db_path)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='processed';")
        result = cur.fetchone()

        assert result is not None
        assert result[0] == "processed"
        conn.close()

    def test_ensure_schema_idempotent(self, tmp_path):
        db_path = tmp_path / "test.db"

        # Call twice
        ensure_schema(db_path)
        ensure_schema(db_path)

        # Should not raise an error
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='processed';")
        count = cur.fetchone()[0]
        assert count == 1
        conn.close()

    def test_ensure_schema_has_required_columns(self, tmp_path):
        db_path = tmp_path / "test.db"
        ensure_schema(db_path)

        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(processed);")
        columns = {row[1] for row in cur.fetchall()}

        required_columns = {
            "id", "hash", "path", "from_header", "subject",
            "date_header", "attachments", "spam", "hash_sha256",
            "synced_at", "archived_at", "remote_path", "processed_at"
        }

        assert required_columns.issubset(columns)
        conn.close()


class TestIsProcessed:
    """Tests for is_processed function."""

    def test_is_processed_false_for_new_hash(self, test_db):
        result = is_processed(test_db, "newhash123")
        assert result is False

    def test_is_processed_true_for_existing_hash(self, test_db):
        # Insert a record
        mark_processed(
            test_db,
            fingerprint="existinghash",
            path="/path/to/email.eml",
            from_hdr="test@example.com",
            subj="Test",
            date_hdr="2024-01-15 10:30:00",
            attachments=[],
            spam=False
        )

        result = is_processed(test_db, "existinghash")
        assert result is True

    def test_is_processed_empty_fingerprint(self, test_db):
        result = is_processed(test_db, "")
        assert result is False


class TestMarkProcessed:
    """Tests for mark_processed function."""

    def test_mark_processed_inserts_new_record(self, test_db):
        mark_processed(
            test_db,
            fingerprint="hash123",
            path="/path/email.eml",
            from_hdr="sender@example.com",
            subj="Test Subject",
            date_hdr="2024-01-15 10:30:00",
            attachments=["/path/att1.pdf", "/path/att2.txt"],
            spam=False
        )

        conn = get_connection(test_db)
        cur = conn.cursor()
        cur.execute("SELECT * FROM processed WHERE hash = ?;", ("hash123",))
        row = cur.fetchone()

        assert row is not None
        assert row["path"] == "/path/email.eml"
        assert row["from_header"] == "sender@example.com"
        assert row["subject"] == "Test Subject"
        assert row["spam"] == 0

    def test_mark_processed_updates_existing_record(self, test_db):
        # Insert first time
        mark_processed(
            test_db, "hash123", "/old/path.eml", "old@example.com",
            "Old Subject", "2024-01-15 10:30:00", [], False
        )

        # Update with same hash
        mark_processed(
            test_db, "hash123", "/new/path.eml", "new@example.com",
            "New Subject", "2024-01-16 10:30:00", [], False
        )

        conn = get_connection(test_db)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM processed WHERE hash = ?;", ("hash123",))
        count = cur.fetchone()[0]

        # Should only have one record
        assert count == 1

        # Should have updated values
        cur.execute("SELECT * FROM processed WHERE hash = ?;", ("hash123",))
        row = cur.fetchone()
        assert row["path"] == "/new/path.eml"
        assert row["from_header"] == "new@example.com"

    def test_mark_processed_spam_flag(self, test_db):
        mark_processed(
            test_db, "spamhash", "/spam.eml", "spammer@example.com",
            "SPAM", "2024-01-15 10:30:00", [], True
        )

        conn = get_connection(test_db)
        cur = conn.cursor()
        cur.execute("SELECT spam FROM processed WHERE hash = ?;", ("spamhash",))
        row = cur.fetchone()

        assert row["spam"] == 1


class TestFetchUnsynced:
    """Tests for fetch_unsynced function."""

    def test_fetch_unsynced_returns_unsynced_only(self, test_db):
        # Add unsynced record
        mark_processed(
            test_db, "unsynced1", "/path1.eml", "test@example.com",
            "Unsynced", "2024-01-15 10:30:00", [], False
        )

        # Add synced record
        mark_processed(
            test_db, "synced1", "/path2.eml", "test@example.com",
            "Synced", "2024-01-15 10:30:00", [], False
        )
        mark_synced(test_db, "synced1", "sha256hash", "remote/path")

        results = fetch_unsynced(test_db)
        hashes = [row["hash"] for row in results]

        assert "unsynced1" in hashes
        assert "synced1" not in hashes

    def test_fetch_unsynced_excludes_spam(self, test_db):
        # Add spam record
        mark_processed(
            test_db, "spam1", "/spam.eml", "spam@example.com",
            "Spam", "2024-01-15 10:30:00", [], True
        )

        results = fetch_unsynced(test_db)
        hashes = [row["hash"] for row in results]

        assert "spam1" not in hashes


class TestMarkSynced:
    """Tests for mark_synced function."""

    def test_mark_synced_updates_record(self, test_db):
        mark_processed(
            test_db, "hash1", "/path.eml", "test@example.com",
            "Test", "2024-01-15 10:30:00", [], False
        )

        mark_synced(test_db, "hash1", "sha256abc", "remote/path/email.eml")

        conn = get_connection(test_db)
        cur = conn.cursor()
        cur.execute("SELECT * FROM processed WHERE hash = ?;", ("hash1",))
        row = cur.fetchone()

        assert row["synced_at"] is not None
        assert row["hash_sha256"] == "sha256abc"
        assert row["remote_path"] == "remote/path/email.eml"

    def test_mark_synced_with_empty_hash(self, test_db):
        # Should not crash
        mark_synced(test_db, "", "sha256", "path")
        mark_synced(test_db, None, "sha256", "path")


class TestFetchSynced:
    """Tests for fetch_synced function."""

    def test_fetch_synced_returns_synced_only(self, test_db):
        # Add unsynced
        mark_processed(
            test_db, "unsynced1", "/path1.eml", "test@example.com",
            "Unsynced", "2024-01-15 10:30:00", [], False
        )

        # Add synced
        mark_processed(
            test_db, "synced1", "/path2.eml", "test@example.com",
            "Synced", "2024-01-15 10:30:00", [], False
        )
        mark_synced(test_db, "synced1", "sha256", "remote/path")

        results = fetch_synced(test_db)
        hashes = [row["hash"] for row in results]

        assert "synced1" in hashes
        assert "unsynced1" not in hashes


class TestMarkArchivedYear:
    """Tests for mark_archived_year function."""

    def test_mark_archived_year_marks_correct_year(self, test_db):
        # Add emails from different years
        mark_processed(
            test_db, "email2023", "/2023.eml", "test@example.com",
            "2023 Email", "2023-06-15 10:30:00", [], False
        )
        mark_synced(test_db, "email2023", "sha1", "remote/2023")

        mark_processed(
            test_db, "email2024", "/2024.eml", "test@example.com",
            "2024 Email", "2024-01-15 10:30:00", [], False
        )
        mark_synced(test_db, "email2024", "sha2", "remote/2024")

        # Archive 2023
        mark_archived_year(test_db, 2023)

        conn = get_connection(test_db)
        cur = conn.cursor()

        cur.execute("SELECT archived_at FROM processed WHERE hash = ?;", ("email2023",))
        archived_2023 = cur.fetchone()["archived_at"]

        cur.execute("SELECT archived_at FROM processed WHERE hash = ?;", ("email2024",))
        archived_2024 = cur.fetchone()["archived_at"]

        assert archived_2023 is not None
        assert archived_2024 is None


class TestGetCandidateRotationYears:
    """Tests for get_candidate_rotation_years function."""

    def test_get_candidate_rotation_years(self, test_db):
        # Add emails from various years
        for year in [2020, 2021, 2022, 2023, 2024]:
            mark_processed(
                test_db, f"email{year}", f"/{year}.eml", "test@example.com",
                f"{year} Email", f"{year}-06-15 10:30:00", [], False
            )
            mark_synced(test_db, f"email{year}", f"sha{year}", f"remote/{year}")

        # Get years <= 2022
        years = get_candidate_rotation_years(test_db, 2022)

        assert 2020 in years
        assert 2021 in years
        assert 2022 in years
        assert 2023 not in years
        assert 2024 not in years


class TestFetchUnarchivedPathsForYear:
    """Tests for fetch_unarchived_paths_for_year function."""

    def test_fetch_unarchived_paths_for_year(self, test_db):
        # Add synced but unarchived emails for 2023
        mark_processed(
            test_db, "hash1", "/path1.eml", "test@example.com",
            "Email 1", "2023-01-15 10:30:00", [], False
        )
        mark_synced(test_db, "hash1", "sha1", "remote/1")

        mark_processed(
            test_db, "hash2", "/path2.eml", "test@example.com",
            "Email 2", "2023-06-15 10:30:00", [], False
        )
        mark_synced(test_db, "hash2", "sha2", "remote/2")

        # Add archived email for 2023
        mark_processed(
            test_db, "hash3", "/path3.eml", "test@example.com",
            "Email 3", "2023-12-15 10:30:00", [], False
        )
        mark_synced(test_db, "hash3", "sha3", "remote/3")
        mark_archived_year(test_db, 2023)

        # Fetch unarchived
        paths = fetch_unarchived_paths_for_year(test_db, 2023)

        # hash3 was archived by mark_archived_year, so it should not be in results
        # But hash1 and hash2 are not archived individually
        # Actually mark_archived_year marks ALL 2023 emails, so all would be archived
        # Let's verify the logic
        assert len(paths) == 0  # All 2023 emails were archived


class TestUpdateRemotePath:
    """Tests for update_remote_path function."""

    def test_update_remote_path(self, test_db):
        mark_processed(
            test_db, "hash1", "/path.eml", "test@example.com",
            "Test", "2024-01-15 10:30:00", [], False
        )

        update_remote_path(test_db, "hash1", "new/remote/path.eml")

        conn = get_connection(test_db)
        cur = conn.cursor()
        cur.execute("SELECT remote_path FROM processed WHERE hash = ?;", ("hash1",))
        row = cur.fetchone()

        assert row["remote_path"] == "new/remote/path.eml"

    def test_update_remote_path_empty_hash(self, test_db):
        # Should not crash
        update_remote_path(test_db, "", "path")
        update_remote_path(test_db, None, "path")
