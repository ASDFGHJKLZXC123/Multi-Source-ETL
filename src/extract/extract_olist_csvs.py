"""
Stage 1c — snapshot the raw Olist CSVs that are NOT modelled in source_system
(reviews, geolocation, category translation) into Bronze as Parquet.

Why this exists
---------------
The six Olist tables loaded into ``source_system`` (customers, stores, products,
orders, order_items, payments) are snapshotted to ``data/bronze/db/<table>/`` by
``src.extract.extract_db``. The remaining three files in the Kaggle bundle are
not loaded into the relational source schema, but the README counts them in the
"9 Olist tables" total. This module gives them an identical Bronze landing so
the count is honest and the raw files are part of the same pipeline lineage.

Files snapshotted
-----------------
- ``olist_order_reviews_dataset.csv``        → ``reviews/snapshot.parquet``
- ``olist_geolocation_dataset.csv``          → ``geolocation/snapshot.parquet``
- ``product_category_name_translation.csv``  → ``category_translation/snapshot.parquet``

Idempotent: re-running overwrites the latest snapshot atomically.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.extract.config import BRONZE_DB
from src.utils.logger import logger

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BRONZE_OLIST = _PROJECT_ROOT / "data" / "bronze" / "olist"

# (csv_stem, bronze_subdir) — payments is now modelled via source_system,
# so it is snapshotted by extract_db and removed from this list to avoid
# two writers in data/bronze/db/payments/ (schema mismatch on read).
_RAW_OLIST_SNAPSHOTS: list[tuple[str, str]] = [
    ("olist_order_reviews_dataset", "reviews"),
    ("olist_geolocation_dataset", "geolocation"),
    ("product_category_name_translation", "category_translation"),
]


def snapshot_one(csv_stem: str, bronze_subdir: str) -> int:
    """Read a single Olist CSV and write it as Parquet under ``data/bronze/db/{subdir}/``.

    Returns the number of rows written, or 0 if the source CSV is absent
    (logged as a warning but not raised — the rest of the pipeline does not
    depend on these files).
    """
    src_path = _BRONZE_OLIST / f"{csv_stem}.csv"
    if not src_path.exists():
        logger.warning("Olist CSV not found, skipping snapshot: {}", src_path)
        return 0

    out_dir = BRONZE_DB / bronze_subdir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "snapshot.parquet"
    tmp_path = out_dir / "snapshot.parquet.tmp"

    df = pd.read_csv(src_path, low_memory=False)
    try:
        df.to_parquet(tmp_path, index=False, engine="pyarrow")
        tmp_path.rename(out_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise

    logger.info("Bronze snapshot ({}): {:,} rows → {}", bronze_subdir, len(df), out_path)
    return len(df)


def snapshot_all() -> dict[str, int]:
    """Snapshot the three unmodelled raw Olist CSVs to Bronze. Returns ``{subdir: row_count}``."""
    results: dict[str, int] = {}
    for csv_stem, bronze_subdir in _RAW_OLIST_SNAPSHOTS:
        results[bronze_subdir] = snapshot_one(csv_stem, bronze_subdir)
    written = sum(1 for n in results.values() if n > 0)
    logger.info("Raw Olist snapshots: {}/{} written", written, len(_RAW_OLIST_SNAPSHOTS))
    return results


__all__ = ["snapshot_one", "snapshot_all"]
