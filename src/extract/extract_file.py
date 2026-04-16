"""
Stage 2 — Flat-File Validation and Ingestion.

Validates and ingests CSV files from ``data/bronze/manual/``.  Covers the
Brazilian municipalities reference file (downloaded on demand) and the seven
Olist dataset CSVs (expected to be placed manually in the same directory).

Usage
-----
    # Validate and ingest all files:
    python -m src.extract.extract_file

    # Validate only (no DataFrame returned, just print results):
    python -m src.extract.extract_file --validate-only

    # Single Olist file:
    python -m src.extract.extract_file --file olist_orders_dataset

    # Re-download municipios if not yet cached, then ingest:
    python -m src.extract.extract_file --file municipios --download-municipios

Programmatic usage:
    from src.extract.extract_file import ingest_olist_file, validate_all_manual_files
    df, is_valid = ingest_olist_file("olist_orders_dataset")
    results = validate_all_manual_files()
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.extract.config import BRONZE_MANUAL, FLAT_FILE_SCHEMAS
from src.extract.extract_flat_files import extract_municipios
from src.utils.logger import logger
from src.utils.validators import log_data_quality_report


# ---------------------------------------------------------------------------
# Public validation helpers
# ---------------------------------------------------------------------------

def validate_flat_file(
    file_path: Path,
    required_columns: list[str],
) -> tuple[bool, list[str]]:
    """Check that a CSV file exists and contains the expected columns.

    Performs a two-pass check:

    1. **Header pass** — reads only 5 rows (fast) to confirm all
       *required_columns* are present.
    2. **Full pass** — reads the entire file and verifies:
       - At least one data row exists.
       - No column is 100 % null across all rows.

    Parameters
    ----------
    file_path : Path
        Absolute path to the CSV file to validate.
    required_columns : list[str]
        Column names that must appear in the file header.

    Returns
    -------
    tuple[bool, list[str]]
        ``(is_valid, issues)`` where *is_valid* is ``True`` only when
        *issues* is empty.
    """
    issues: list[str] = []

    if not file_path.exists():
        issues.append(f"File not found: {file_path}")
        return False, issues

    # --- Pass 1: header check (nrows=5 for speed) ---
    try:
        header_df = pd.read_csv(file_path, nrows=5)
    except Exception as exc:
        issues.append(f"Could not read file header: {exc}")
        return False, issues

    missing_cols = [c for c in required_columns if c not in header_df.columns]
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")
        return False, issues

    # --- Pass 2: full file checks ---
    try:
        full_df = pd.read_csv(file_path, low_memory=False)
    except Exception as exc:
        issues.append(f"Could not read full file: {exc}")
        return False, issues

    if len(full_df) == 0:
        issues.append("File contains no data rows.")
        return False, issues

    for col in required_columns:
        if col not in full_df.columns:
            # Defensive — should have been caught in pass 1.
            issues.append(f"Column '{col}' disappeared in full read.")
            continue
        null_rate = full_df[col].isna().mean()
        if null_rate == 1.0:
            issues.append(f"Column '{col}' is 100% null.")

    is_valid = len(issues) == 0
    return is_valid, issues


# ---------------------------------------------------------------------------
# Municipios ingestion
# ---------------------------------------------------------------------------

def ingest_municipios(force: bool = False) -> tuple[pd.DataFrame, bool]:
    """Download (if needed) and validate the municipalities reference file.

    Calls :func:`src.extract.extract_flat_files.extract_municipios` which
    handles download-and-cache logic, then validates the result against
    :data:`FLAT_FILE_SCHEMAS` ``["municipios"]``.

    Parameters
    ----------
    force : bool
        When ``True``, the cached CSV is deleted before calling
        ``extract_municipios`` so a fresh download is triggered.

    Returns
    -------
    tuple[pd.DataFrame, bool]
        ``(df, is_valid)``.  On download or read failure *df* will be empty
        and *is_valid* will be ``False``.
    """
    if force:
        cached = BRONZE_MANUAL / "municipios.csv"
        if cached.exists():
            cached.unlink()
            logger.info("force=True: deleted cached municipios file at {}", cached)

    try:
        df = extract_municipios()
    except Exception as exc:
        logger.error("extract_municipios() raised an error: {}", exc)
        return pd.DataFrame(), False

    required_columns = FLAT_FILE_SCHEMAS.get("municipios", [])
    municipios_path = BRONZE_MANUAL / "municipios.csv"
    is_valid, issues = validate_flat_file(municipios_path, required_columns)

    if is_valid:
        logger.info(
            "Municipios validation PASSED ({:,} rows, {} required columns present)",
            len(df), len(required_columns),
        )
    else:
        for issue in issues:
            logger.warning("Municipios validation issue: {}", issue)
        logger.warning("Municipios validation FAILED ({} issue(s))", len(issues))

    return df, is_valid


# ---------------------------------------------------------------------------
# Olist file ingestion
# ---------------------------------------------------------------------------

def ingest_olist_file(filename_stem: str) -> tuple[pd.DataFrame | None, bool]:
    """Read and validate a single Olist dataset CSV.

    Parameters
    ----------
    filename_stem : str
        File name without extension, e.g. ``"olist_orders_dataset"``.
        The function looks for ``data/bronze/manual/{filename_stem}.csv``.

    Returns
    -------
    tuple[pd.DataFrame | None, bool]
        ``(df, is_valid)``.  Returns ``(None, False)`` when the file is
        absent.  When validation fails *df* is still returned (so the caller
        can inspect the data), but *is_valid* is ``False``.
    """
    file_path = BRONZE_MANUAL / f"{filename_stem}.csv"

    if not file_path.exists():
        logger.warning(
            "Olist file not found: {} — skipping ingestion", file_path
        )
        return None, False

    required_columns = FLAT_FILE_SCHEMAS.get(filename_stem, [])
    is_valid, issues = validate_flat_file(file_path, required_columns)

    if not is_valid:
        for issue in issues:
            logger.warning("[{}] validation issue: {}", filename_stem, issue)
        logger.warning(
            "[{}] validation FAILED ({} issue(s))", filename_stem, len(issues)
        )

    # Read the full DataFrame regardless of validation outcome so the quality
    # report and return value are populated for diagnostic purposes.
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as exc:
        logger.error("Could not read {}: {}", file_path, exc)
        return None, False

    log_data_quality_report(df, filename_stem)

    if is_valid:
        logger.info(
            "[{}] validation PASSED ({:,} rows, {} required columns present)",
            filename_stem, len(df), len(required_columns),
        )

    return df, is_valid


# ---------------------------------------------------------------------------
# Bulk validation
# ---------------------------------------------------------------------------

def validate_all_manual_files() -> dict[str, bool]:
    """Validate every file registered in :data:`FLAT_FILE_SCHEMAS`.

    For the ``"municipios"`` entry the validation is run against the cached
    CSV directly (no download is triggered here — call
    :func:`ingest_municipios` first if the file may not yet exist).

    All other entries are treated as Olist CSVs expected to be present in
    ``data/bronze/manual/``.

    Returns
    -------
    dict[str, bool]
        Mapping of ``filename_stem -> is_valid`` for every schema entry.
    """
    results: dict[str, bool] = {}

    for filename_stem, required_columns in FLAT_FILE_SCHEMAS.items():
        file_path = BRONZE_MANUAL / f"{filename_stem}.csv"
        is_valid, issues = validate_flat_file(file_path, required_columns)
        results[filename_stem] = is_valid

        status_label = "PASS" if is_valid else "FAIL"
        if is_valid:
            logger.info("  {:45s}  {}", filename_stem, status_label)
        else:
            logger.warning("  {:45s}  {}", filename_stem, status_label)
            for issue in issues:
                logger.warning("    -> {}", issue)

    # Summary table
    total = len(results)
    passed = sum(results.values())
    failed = total - passed

    logger.info("-" * 55)
    logger.info(
        "Flat-file validation summary: {}/{} PASS, {}/{} FAIL",
        passed, total, failed, total,
    )

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _all_file_choices() -> list[str]:
    """Return the list of valid ``--file`` argument choices."""
    return ["all", "municipios"] + [
        stem for stem in FLAT_FILE_SCHEMAS if stem != "municipios"
    ]


def _build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="extract_file",
        description="Stage 2 — Validate and ingest flat files from data/bronze/manual/.",
    )
    parser.add_argument(
        "--file",
        choices=_all_file_choices(),
        default="all",
        help="Which file(s) to process (default: all).",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        default=False,
        help="Validate files without returning/storing DataFrames.",
    )
    parser.add_argument(
        "--download-municipios",
        action="store_true",
        default=False,
        help="Trigger a fresh download of the municipios CSV before processing.",
    )
    return parser


def _print_validation_summary(results: dict[str, bool]) -> None:
    """Print a human-readable validation table to stdout."""
    print("\n" + "=" * 60)
    print("  Flat-File Validation Results")
    print("=" * 60)
    for stem, is_valid in results.items():
        label = "PASS" if is_valid else "FAIL"
        print(f"  {stem:<45s}  {label}")
    print("=" * 60)
    passed = sum(results.values())
    total = len(results)
    print(f"  {passed}/{total} files passed\n")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Parameters
    ----------
    argv : list[str] | None
        Argument list override (defaults to ``sys.argv[1:]``).

    Returns
    -------
    int
        Exit code: 0 when all validated files pass, 1 if any fail.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    target: str = args.file
    validate_only: bool = args.validate_only
    download_municipios: bool = args.download_municipios

    results: dict[str, bool] = {}

    if target == "all":
        # Ensure municipios is present before bulk validation runs.
        if download_municipios:
            _, muni_valid = ingest_municipios(force=True)
        elif not validate_only:
            _, muni_valid = ingest_municipios(force=False)

        if validate_only:
            results = validate_all_manual_files()
        else:
            # Ingest each Olist file individually (provides quality reports).
            for filename_stem in FLAT_FILE_SCHEMAS:
                if filename_stem == "municipios":
                    # Already handled above; just record the validity.
                    muni_path = BRONZE_MANUAL / "municipios.csv"
                    required = FLAT_FILE_SCHEMAS.get("municipios", [])
                    is_valid, _ = validate_flat_file(muni_path, required)
                    results["municipios"] = is_valid
                    continue
                _, is_valid = ingest_olist_file(filename_stem)
                results[filename_stem] = is_valid

    elif target == "municipios":
        force = download_municipios
        if validate_only:
            muni_path = BRONZE_MANUAL / "municipios.csv"
            required = FLAT_FILE_SCHEMAS.get("municipios", [])
            is_valid, issues = validate_flat_file(muni_path, required)
            if not is_valid:
                for issue in issues:
                    logger.warning("Municipios: {}", issue)
            results["municipios"] = is_valid
        else:
            _, is_valid = ingest_municipios(force=force)
            results["municipios"] = is_valid

    else:
        # Specific Olist file.
        if validate_only:
            file_path = BRONZE_MANUAL / f"{target}.csv"
            required = FLAT_FILE_SCHEMAS.get(target, [])
            is_valid, issues = validate_flat_file(file_path, required)
            if not is_valid:
                for issue in issues:
                    logger.warning("[{}]: {}", target, issue)
            results[target] = is_valid
        else:
            _, is_valid = ingest_olist_file(target)
            results[target] = is_valid

    _print_validation_summary(results)

    any_failed = not all(results.values())
    return 1 if any_failed else 0


__all__ = [
    "validate_flat_file",
    "ingest_municipios",
    "ingest_olist_file",
    "validate_all_manual_files",
]

if __name__ == "__main__":
    sys.exit(main())
