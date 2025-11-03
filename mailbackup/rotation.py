#!/usr/bin/env python3

"""
rotation.py

Rotation step (immutable archive model):
- determine years to archive
- download existing year archive
- merge with current remote content for that year
- recompress to tar.zst
- upload
- mark archived in DB
- update manifest
"""

from __future__ import annotations

import datetime
import json
import shutil

from mailbackup import db
from mailbackup.config import Settings
from mailbackup.logger import get_logger
from mailbackup.manifest import ManifestManager
from mailbackup.rclone import rclone_copy, rclone_lsf
from mailbackup.utils import run_cmd, sha256, safe_write_json, atomic_upload_file


def rotate_archives(settings: Settings, manifest: ManifestManager, stats: dict) -> None:
    """
    Perform immutable-year rotation for archived emails.

    Behaviour:
    - Determine candidate years older than retention threshold
    - Download existing year archive (if any), extract it
    - Fetch current remote year folder (excluding internal _archives)
    - Merge existing archive contents with current remote files
    - Recreate a compressed tar.zst archive atomically and upload
    - Mark entries in DB as archived and queue the archive in the manifest

    The function is best-effort and will skip a year on errors.
    """
    # Use central logger instead of caller-provided logger
    logger = get_logger(__name__)
    logger.info("Starting immutable rotation (untar + append model)...")

    current_year = datetime.datetime.now(datetime.timezone.utc).year
    target_year = current_year - settings.retention_years

    # Use centralized db helper to get candidate years
    candidate_years = db.get_candidate_rotation_years(settings.db_path, target_year)

    if not candidate_years:
        logger.info("No synced emails eligible for rotation.")
        return

    logger.info(f"Candidate years for rotation: {', '.join(map(str, candidate_years))}")
    years = sorted(candidate_years)
    logger.info(f"Starting rotation for {len(years)} years...")

    for idx, year in enumerate(years, start=1):
        logger.info(f"Processing archive for year {year}...")

        # fetch unarchived rows using db helper
        unarchived_paths = db.fetch_unarchived_paths_for_year(settings.db_path, year)
        missing_count = len(unarchived_paths)

        existing_remote_archive = f"{settings.remote}/{year}/_archives/emails_{year}.tar.zst"
        local_year_dir = settings.tmp_dir / "rotation" / str(year)
        local_year_dir.mkdir(parents=True, exist_ok=True)
        archive_file = local_year_dir / f"emails_{year}.tar.zst"

        if missing_count == 0:
            result = rclone_lsf(existing_remote_archive, check=False)
            if result.returncode == 0:
                logger.info(f"Year {year}: archive already complete. Skipping.")
                shutil.rmtree(local_year_dir, ignore_errors=True)
                continue

        logger.info(f"Year {year}: {missing_count} new emails to append (if archive exists).")

        extracted_dir = local_year_dir / "extracted"
        merged_dir = local_year_dir / "merged"
        new_dir = local_year_dir / "new"
        for d in [extracted_dir, merged_dir, new_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Step 1: download existing archive
        existing_archive_downloaded = False
        rclone_copy(
            f"{settings.remote}/{year}/_archives/",
            str(local_year_dir),
            "--include",
            f"emails_{year}.tar.zst",
            check=False,
        )
        if archive_file.exists() and archive_file.stat().st_size > 0:
            existing_archive_downloaded = True
            logger.info(f"Existing archive for {year} downloaded successfully.")
        else:
            logger.warning(f"Archive file for {year} not found or empty after download.")

        # Step 2: extract
        if existing_archive_downloaded:
            logger.info(f"Extracting old archive for {year}...")
            run_cmd("tar", "-I", "zstd", "-xf", str(archive_file), "-C", str(extracted_dir), check=True)
        else:
            logger.info(f"No previous archive content for {year}.")

        # Step 3: fetch all remote emails for this year
        remote_year_dir = f"{settings.remote}/{year}"
        res = rclone_copy(
            remote_year_dir,
            str(new_dir),
            "--exclude",
            "_archives/**",
            check=False,
        )
        if res.returncode != 0:
            logger.warning(f"Warning: could not fetch remote year {year}. Skipping.")
            shutil.rmtree(local_year_dir, ignore_errors=True)
            continue

        # Step 4: merge
        logger.info(f"Merging previous archive and new emails for {year}...")
        for src in extracted_dir.rglob("*"):
            if src.is_file():
                dest = merged_dir / src.relative_to(extracted_dir)
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists():
                    shutil.copy2(src, dest)
        for src in new_dir.rglob("*"):
            if src.is_file():
                dest = merged_dir / src.relative_to(new_dir)
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists():
                    shutil.copy2(src, dest)

        # Step 4b: update info.json in merged_dir
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        updated_files = 0
        for info_path in merged_dir.rglob("info.json"):
            try:
                with open(info_path, "r", encoding="utf-8") as f:
                    info_data = json.load(f)
                info_data["archived_at"] = now_iso
                info_data["metadata_version"] = info_data.get("metadata_version", 1)
                info_data["archive_name"] = f"emails_{year}.tar.zst"
                safe_write_json(info_path, info_data)
                updated_files += 1
            except Exception as e:
                logger.warning(f"Warning: could not update info.json {info_path}: {e}")

        logger.info(f"Updated archived_at for {updated_files} info.json files in {year}.")

        # Step 5: recompress
        new_archive = local_year_dir / f"emails_{year}.tar.zst"
        logger.info(f"Compressing merged archive for {year}...")
        run_cmd("tar", "-I", "zstd -T0", "-cf", str(new_archive), "-C", str(merged_dir), ".", check=True)

        # Step 6: upload archive atomically (copyto tmp -> moveto final)
        remote_archive_final = f"{settings.remote}/{year}/_archives/emails_{year}.tar.zst"
        # Use centralized atomic upload helper for archives (copyto tmp -> moveto final)
        if not atomic_upload_file(new_archive, remote_archive_final):
            logger.error(f"Error: failed to upload/move archive into place for year {year}")
            shutil.rmtree(local_year_dir, ignore_errors=True)
            continue

        # Step 7: mark archived in DB
        db.mark_archived_year(settings.db_path, year)
        archive_hash = sha256(new_archive)
        logger.info(f"Database updated for year {year} (archived_at set).")

        # Step 8: sync remote info.json updates
        rclone_copy(
            str(merged_dir),
            f"{settings.remote}/{year}",
            "--include",
            "**/info.json",
            check=False,
        )

        # Step 9: update manifest
        manifest.queue_entry(f"{year}/_archives/emails_{year}.tar.zst", archive_hash)

        logger.info(f"Year {year} rotation complete (archive + metadata synced).")
        shutil.rmtree(local_year_dir, ignore_errors=True)

        stats["archived"] = stats.get("archived", 0) + 1
        logger.info(f"[Progress] Archived {idx}/{len(years)} years ({len(years) - idx} remaining).")

    manifest.upload_manifest_if_needed()
    logger.info("Rotation complete.")
    logger.info("[STATUS] Uploaded: {u} | Archived: {a} | Verified: {v} | Repaired: {r} | Skipped: {s}".format(
        u=stats.get("uploaded", 0),
        a=stats.get("archived", 0),
        v=stats.get("verified", 0),
        r=stats.get("repaired", 0),
        s=stats.get("skipped", 0),
    ))
