"""
Stage 1 — Source System Setup: one-time loader for source_system tables.

Downloads the Olist Brazilian E-Commerce dataset from Kaggle and loads
it into the source_system PostgreSQL schema with enterprise-style transformations:
  - Integer surrogate PKs (SERIAL) instead of UUID string keys
  - Customers deduplicated on business key (customer_unique_id)
  - Sellers mapped to stores with Brazilian region derivation
  - Products joined with English category translations
  - Orders linked to customers via FK; timestamps split into date + timestamp
  - Order items with all FK references resolved to integer keys

Re-running is safe: tables already containing data are skipped.

Usage
-----
    python -m src.setup.load_source_db
    python main.py --stage setup
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tqdm import tqdm

from src.utils.db import get_connection, get_engine
from src.utils.logger import logger
from src.utils.validators import log_data_quality_report

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BRONZE_OLIST = _PROJECT_ROOT / "data" / "bronze" / "olist"
_DDL_PATH = _PROJECT_ROOT / "sql" / "ddl" / "03_source_system.sql"

# ---------------------------------------------------------------------------
# Brazilian state → region mapping used for stores.region
# ---------------------------------------------------------------------------
_BRAZIL_REGION_MAP: dict[str, str] = {
    "AC": "Norte",       "AM": "Norte",        "AP": "Norte",       "PA": "Norte",
    "RO": "Norte",       "RR": "Norte",        "TO": "Norte",
    "AL": "Nordeste",    "BA": "Nordeste",     "CE": "Nordeste",    "MA": "Nordeste",
    "PB": "Nordeste",    "PE": "Nordeste",     "PI": "Nordeste",    "RN": "Nordeste",
    "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste",     "MG": "Sudeste",      "RJ": "Sudeste",     "SP": "Sudeste",
    "PR": "Sul",         "RS": "Sul",          "SC": "Sul",
}

# Generated columns that PostgreSQL computes automatically — never include in inserts.
_GENERATED_COLUMNS = {"delivery_days_actual", "delivery_days_estimated"}


# ---------------------------------------------------------------------------
# Step 1 — Kaggle download
# ---------------------------------------------------------------------------

def download_olist_data() -> None:
    """Download Olist CSVs via kagglehub if not already present.

    Idempotent: exits immediately if CSV files are already present in
    ``data/bronze/olist/``.  Requires ``~/.kaggle/kaggle.json`` or the
    ``KAGGLE_USERNAME`` / ``KAGGLE_KEY`` environment variables.
    """
    _BRONZE_OLIST.mkdir(parents=True, exist_ok=True)

    existing_csvs = list(_BRONZE_OLIST.glob("*.csv"))
    if existing_csvs:
        logger.info(
            "Olist data already present ({} CSV files in {}). Skipping download.",
            len(existing_csvs),
            _BRONZE_OLIST,
        )
        return

    logger.info("Downloading Olist dataset from Kaggle via kagglehub...")
    try:
        import kagglehub  # type: ignore

        download_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
        source_dir = Path(download_path)
        logger.info("Kaggle download completed. Source path: {}", source_dir)

        csv_files = list(source_dir.rglob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"No CSV files found in kagglehub download path: {source_dir}"
            )

        for csv_file in tqdm(csv_files, desc="Copying CSVs to bronze/olist"):
            destination = _BRONZE_OLIST / csv_file.name
            shutil.copy2(csv_file, destination)
            logger.debug("Copied {} → {}", csv_file.name, destination)

        logger.info(
            "Olist dataset ready: {} CSV files copied to {}",
            len(csv_files),
            _BRONZE_OLIST,
        )

    except ImportError:
        logger.error("kagglehub is not installed. Run: pip install kagglehub")
        raise
    except Exception as exc:
        logger.error("Failed to download Olist data: {}", exc)
        raise


# ---------------------------------------------------------------------------
# Step 2 — DDL execution
# ---------------------------------------------------------------------------

def create_source_tables() -> None:
    """Execute sql/ddl/03_source_system.sql to create enterprise schema objects.

    Reads ``_DDL_PATH`` and executes it via a raw psycopg2 connection so that
    multi-statement DDL scripts (with ``IF NOT EXISTS``) work correctly.
    This function is fully idempotent.
    """
    if not _DDL_PATH.exists():
        raise FileNotFoundError(
            f"DDL script not found: {_DDL_PATH}. "
            "Ensure sql/ddl/03_source_system.sql is present."
        )

    logger.info("Executing DDL: {}", _DDL_PATH.name)
    sql = _DDL_PATH.read_text()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

    logger.info("DDL complete — source_system schema objects are ready.")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _table_has_data(engine: Engine, schema: str, table_name: str) -> bool:
    """Return True if ``schema.table_name`` already contains at least one row.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    schema : str
        PostgreSQL schema name.
    table_name : str
        Table name within that schema.
    """
    sql = f"SELECT EXISTS (SELECT 1 FROM {schema}.{table_name} LIMIT 1)"
    with engine.connect() as conn:
        result = conn.execute(text(sql)).scalar()
    return bool(result)


def _read_csv(csv_stem: str) -> pd.DataFrame:
    """Read a CSV from ``data/bronze/olist/`` into a DataFrame.

    Parameters
    ----------
    csv_stem : str
        File stem without the ``.csv`` extension.

    Raises
    ------
    FileNotFoundError
        If the file does not exist in ``_BRONZE_OLIST``.
    """
    path = _BRONZE_OLIST / f"{csv_stem}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Expected CSV not found: {path}. "
            "Run download_olist_data() first or place files manually."
        )
    df = pd.read_csv(path, low_memory=False)
    logger.debug("Read {:,} rows from {}", len(df), path.name)
    return df


def _add_ingested_at(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *df* with an ``ingested_at`` column set to now."""
    df = df.copy()
    df["ingested_at"] = pd.Timestamp.now()
    return df


def _drop_generated_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop any PostgreSQL GENERATED columns so they are not passed to INSERT."""
    cols_to_drop = _GENERATED_COLUMNS.intersection(df.columns)
    if cols_to_drop:
        logger.debug("Dropping generated columns before insert: {}", cols_to_drop)
        df = df.drop(columns=list(cols_to_drop))
    return df


def _batch_insert(
    engine: Engine,
    df: pd.DataFrame,
    table_name: str,
    schema: str = "source_system",
    chunksize: int = 500,
) -> None:
    """Insert *df* into ``schema.table_name`` in chunks, reporting progress via tqdm.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    df : pd.DataFrame
        Fully prepared DataFrame (ingested_at added, generated columns dropped).
    table_name : str
        Target table name (without schema prefix).
    schema : str
        Target schema name.
    chunksize : int
        Rows per INSERT batch.
    """
    df = _drop_generated_columns(df)
    total_chunks = max(1, -(-len(df) // chunksize))  # ceiling division

    with tqdm(total=len(df), desc=f"Loading {table_name}", unit="rows", leave=False) as pbar:
        for i in range(total_chunks):
            chunk = df.iloc[i * chunksize : (i + 1) * chunksize]
            chunk.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=chunksize,
            )
            pbar.update(len(chunk))


# ---------------------------------------------------------------------------
# Step 3 — Individual table loaders
# ---------------------------------------------------------------------------

def load_customers() -> dict[str, int]:
    """Load customers into source_system.customers.

    Deduplicates on ``customer_unique_id`` (the business key for a physical
    person — Olist issues a different ``customer_id`` UUID per order for the
    same shopper).

    Returns
    -------
    dict[str, int]
        ``{customer_code: customer_id}`` surrogate-key lookup for FK resolution
        in subsequent steps.
    """
    engine = get_engine()

    # Build lookup from already-loaded rows regardless of skip/insert path.
    def _build_lookup() -> dict[str, int]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT customer_code, customer_id FROM source_system.customers")
            ).fetchall()
        return {row.customer_code: row.customer_id for row in rows}

    if _table_has_data(engine, "source_system", "customers"):
        logger.info("source_system.customers already has data — skipping load.")
        return _build_lookup()

    logger.info("--- Loading source_system.customers ---")
    try:
        raw = _read_csv("olist_customers_dataset")
        log_data_quality_report(raw, "customers_raw")

        # Deduplicate on the business key; keep first occurrence.
        df = raw.drop_duplicates(subset=["customer_unique_id"]).copy()
        before, after = len(raw), len(df)
        if before != after:
            logger.info(
                "customers: deduplicated {:,} → {:,} rows on customer_unique_id",
                before,
                after,
            )

        # Map to enterprise column names.
        df = df.rename(columns={
            "customer_unique_id":        "customer_code",
            "customer_zip_code_prefix":  "zip_code_prefix",
            "customer_city":             "city",
            "customer_state":            "state",
        })

        # Enterprise schema columns only (SERIAL PK excluded — PostgreSQL assigns it).
        keep = ["customer_code", "zip_code_prefix", "city", "state"]
        df = df[keep].copy()
        df["is_active"] = True
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()
        df = _add_ingested_at(df)

        _batch_insert(engine, df, "customers")
        logger.info("Loaded {:,} rows into source_system.customers", len(df))

    except Exception as exc:
        logger.error("Failed to load source_system.customers: {}", exc)
        raise

    return _build_lookup()


def load_stores() -> dict[str, int]:
    """Load sellers into source_system.stores with Brazilian region derivation.

    ``region`` is derived from the seller's state abbreviation using
    ``_BRAZIL_REGION_MAP``.  States not present in the map receive ``'Desconhecido'``.

    Returns
    -------
    dict[str, int]
        ``{store_code: store_id}`` surrogate-key lookup.
    """
    engine = get_engine()

    def _build_lookup() -> dict[str, int]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT store_code, store_id FROM source_system.stores")
            ).fetchall()
        return {row.store_code: row.store_id for row in rows}

    if _table_has_data(engine, "source_system", "stores"):
        logger.info("source_system.stores already has data — skipping load.")
        return _build_lookup()

    logger.info("--- Loading source_system.stores ---")
    try:
        raw = _read_csv("olist_sellers_dataset")
        log_data_quality_report(raw, "stores_raw")

        df = raw.drop_duplicates(subset=["seller_id"]).copy()

        df = df.rename(columns={
            "seller_id":               "store_code",
            "seller_zip_code_prefix":  "zip_code_prefix",
            "seller_city":             "city",
            "seller_state":            "state",
        })

        df["region"] = df["state"].str.upper().map(_BRAZIL_REGION_MAP).fillna("Desconhecido")

        unknown_states = df.loc[df["region"] == "Desconhecido", "state"].unique()
        if len(unknown_states):
            logger.warning(
                "stores: {} state code(s) not in region map: {}",
                len(unknown_states),
                list(unknown_states),
            )

        keep = ["store_code", "zip_code_prefix", "city", "state", "region"]
        df = df[keep].copy()
        df["is_active"] = True
        df["created_at"] = pd.Timestamp.now()
        df = _add_ingested_at(df)

        _batch_insert(engine, df, "stores")
        logger.info("Loaded {:,} rows into source_system.stores", len(df))

    except Exception as exc:
        logger.error("Failed to load source_system.stores: {}", exc)
        raise

    return _build_lookup()


def load_products() -> dict[str, int]:
    """Load products into source_system.products with English category names.

    Joins ``olist_products_dataset.csv`` with
    ``product_category_name_translation.csv`` on ``product_category_name``.
    Three categories in the dataset have no English translation; those receive
    the Portuguese name as their English name (``fillna`` strategy).

    Returns
    -------
    dict[str, int]
        ``{product_code: product_id}`` surrogate-key lookup.
    """
    engine = get_engine()

    def _build_lookup() -> dict[str, int]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT product_code, product_id FROM source_system.products")
            ).fetchall()
        return {row.product_code: row.product_id for row in rows}

    if _table_has_data(engine, "source_system", "products"):
        logger.info("source_system.products already has data — skipping load.")
        return _build_lookup()

    logger.info("--- Loading source_system.products ---")
    try:
        raw_products = _read_csv("olist_products_dataset")
        raw_translations = _read_csv("product_category_name_translation")
        log_data_quality_report(raw_products, "products_raw")

        df = raw_products.drop_duplicates(subset=["product_id"]).copy()

        # Join with translations (left join to keep products lacking a category).
        df = df.merge(
            raw_translations[["product_category_name", "product_category_name_english"]],
            on="product_category_name",
            how="left",
        )

        # Three categories have no English translation — fall back to Portuguese name.
        df["product_category_name_english"] = (
            df["product_category_name_english"]
            .fillna(df["product_category_name"])
            .fillna("unknown")
        )

        # Rename to enterprise schema column names.
        df = df.rename(columns={
            "product_id":                    "product_code",
            "product_category_name":         "category_name_pt",
            "product_category_name_english": "category_name_en",
            "product_weight_g":              "weight_g",
            "product_length_cm":             "length_cm",
            "product_height_cm":             "height_cm",
            "product_width_cm":              "width_cm",
        })

        # product_name_lenght / product_description_lenght are Olist-only columns
        # that do not exist in the enterprise schema — intentionally excluded.
        keep = [
            "product_code",
            "category_name_pt",
            "category_name_en",
            "weight_g",
            "length_cm",
            "height_cm",
            "width_cm",
        ]
        df = df[keep].copy()
        df["is_active"] = True
        df["created_at"] = pd.Timestamp.now()
        df = _add_ingested_at(df)

        _batch_insert(engine, df, "products")
        logger.info("Loaded {:,} rows into source_system.products", len(df))

    except Exception as exc:
        logger.error("Failed to load source_system.products: {}", exc)
        raise

    return _build_lookup()


def load_orders(customer_lookup: dict[str, int]) -> dict[str, int]:
    """Load orders into source_system.orders with FK resolution to customers.

    Olist's ``orders.customer_id`` is a per-order UUID that maps to a single
    row in ``olist_customers_dataset.csv``; it is NOT the ``customer_unique_id``
    (business key for a physical person).  This function joins orders to the
    customers CSV to obtain the ``customer_unique_id``, then resolves it to the
    integer ``customer_id`` FK using *customer_lookup*.

    Parameters
    ----------
    customer_lookup : dict[str, int]
        ``{customer_code: customer_id}`` mapping returned by :func:`load_customers`.

    Returns
    -------
    dict[str, int]
        ``{order_code: order_id}`` surrogate-key lookup.
    """
    engine = get_engine()

    def _build_lookup() -> dict[str, int]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT order_code, order_id FROM source_system.orders")
            ).fetchall()
        return {row.order_code: row.order_id for row in rows}

    if _table_has_data(engine, "source_system", "orders"):
        logger.info("source_system.orders already has data — skipping load.")
        return _build_lookup()

    logger.info("--- Loading source_system.orders ---")
    try:
        raw_orders = _read_csv("olist_orders_dataset")
        raw_customers = _read_csv("olist_customers_dataset")
        log_data_quality_report(raw_orders, "orders_raw")

        # Join per-order customer_id → customer_unique_id.
        cust_bridge = raw_customers[["customer_id", "customer_unique_id"]].drop_duplicates(
            subset=["customer_id"]
        )
        df = raw_orders.merge(cust_bridge, on="customer_id", how="left")

        missing_unique_id = df["customer_unique_id"].isna().sum()
        if missing_unique_id:
            logger.warning(
                "orders: {:,} rows have no matching customer_unique_id after join — will be dropped.",
                missing_unique_id,
            )
            df = df.dropna(subset=["customer_unique_id"])

        # Resolve customer_unique_id → integer FK.
        df["customer_id_int"] = df["customer_unique_id"].map(customer_lookup)
        fk_miss_mask = df["customer_id_int"].isna()
        fk_miss_count = fk_miss_mask.sum()
        if fk_miss_count:
            sample = df.loc[fk_miss_mask, "customer_unique_id"].head(5).tolist()
            logger.warning(
                "orders: {:,} rows reference a customer_unique_id not in the "
                "customers table — skipping. Sample keys: {}",
                fk_miss_count,
                sample,
            )
            df = df[~fk_miss_mask].copy()

        df["customer_id"] = df["customer_id_int"].astype(int)

        # Timestamp parsing.
        df["order_purchase_timestamp"] = pd.to_datetime(
            df["order_purchase_timestamp"], errors="coerce"
        )
        df["order_date"] = df["order_purchase_timestamp"].dt.date
        df["order_timestamp"] = df["order_purchase_timestamp"]

        df["approved_at"] = pd.to_datetime(df["order_approved_at"], errors="coerce")

        df["estimated_delivery"] = pd.to_datetime(
            df["order_estimated_delivery_date"], errors="coerce"
        ).dt.date

        df["actual_delivery"] = pd.to_datetime(
            df["order_delivered_customer_date"], errors="coerce"
        ).dt.date

        # Rename and select enterprise columns.
        df = df.rename(columns={"order_id": "order_code"})

        keep = [
            "order_code",
            "customer_id",
            "order_status",
            "order_date",
            "order_timestamp",
            "approved_at",
            "estimated_delivery",
            "actual_delivery",
        ]
        df = df[keep].copy()
        df["source_channel"] = "online"
        df["currency_code"] = "BRL"
        df["created_at"] = pd.Timestamp.now()
        df = _add_ingested_at(df)

        # Drop any GENERATED columns before insert.
        df = _drop_generated_columns(df)

        _batch_insert(engine, df, "orders")
        logger.info("Loaded {:,} rows into source_system.orders", len(df))

    except Exception as exc:
        logger.error("Failed to load source_system.orders: {}", exc)
        raise

    return _build_lookup()


def load_order_items(
    order_lookup: dict[str, int],
    product_lookup: dict[str, int],
    store_lookup: dict[str, int],
) -> int:
    """Load order items into source_system.order_items with all FK references resolved.

    Parameters
    ----------
    order_lookup : dict[str, int]
        ``{order_code: order_id}`` returned by :func:`load_orders`.
    product_lookup : dict[str, int]
        ``{product_code: product_id}`` returned by :func:`load_products`.
    store_lookup : dict[str, int]
        ``{store_code: store_id}`` returned by :func:`load_stores`.

    Returns
    -------
    int
        Number of rows successfully inserted (0 if the table was skipped).
    """
    engine = get_engine()

    if _table_has_data(engine, "source_system", "order_items"):
        logger.info("source_system.order_items already has data — skipping load.")
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM source_system.order_items")
            ).scalar()
        return int(count or 0)

    logger.info("--- Loading source_system.order_items ---")
    try:
        raw = _read_csv("olist_order_items_dataset")
        log_data_quality_report(raw, "order_items_raw")

        df = raw.drop_duplicates(subset=["order_id", "order_item_id"]).copy()
        original_count = len(df)

        # Resolve order FK.
        df["order_id_int"] = df["order_id"].map(order_lookup)
        miss_orders = df["order_id_int"].isna()
        if miss_orders.sum():
            sample = df.loc[miss_orders, "order_id"].head(5).tolist()
            logger.warning(
                "order_items: {:,} rows reference unknown order_id — skipping. Sample: {}",
                miss_orders.sum(),
                sample,
            )
            df = df[~miss_orders].copy()

        # Resolve product FK.
        df["product_id_int"] = df["product_id"].map(product_lookup)
        miss_products = df["product_id_int"].isna()
        if miss_products.sum():
            sample = df.loc[miss_products, "product_id"].head(5).tolist()
            logger.warning(
                "order_items: {:,} rows reference unknown product_id — skipping. Sample: {}",
                miss_products.sum(),
                sample,
            )
            df = df[~miss_products].copy()

        # Resolve store FK.
        df["store_id_int"] = df["seller_id"].map(store_lookup)
        miss_stores = df["store_id_int"].isna()
        if miss_stores.sum():
            sample = df.loc[miss_stores, "seller_id"].head(5).tolist()
            logger.warning(
                "order_items: {:,} rows reference unknown seller_id — skipping. Sample: {}",
                miss_stores.sum(),
                sample,
            )
            df = df[~miss_stores].copy()

        skipped = original_count - len(df)
        if skipped:
            logger.info(
                "order_items: {:,} rows skipped due to unresolved FK references ({:,} remaining)",
                skipped,
                len(df),
            )

        # Cast resolved integer FKs.
        df["order_id"] = df["order_id_int"].astype(int)
        df["product_id"] = df["product_id_int"].astype(int)
        df["store_id"] = df["store_id_int"].astype(int)

        df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")

        # Rename to enterprise schema column names.
        df = df.rename(columns={
            "order_item_id": "line_number",
            "price":         "unit_price",
        })

        keep = [
            "order_id",
            "product_id",
            "store_id",
            "line_number",
            "unit_price",
            "freight_value",
            "shipping_limit_date",
        ]
        df = df[keep].copy()
        df["quantity"] = 1
        df["created_at"] = pd.Timestamp.now()
        df = _add_ingested_at(df)

        _batch_insert(engine, df, "order_items")
        logger.info("Loaded {:,} rows into source_system.order_items", len(df))
        return len(df)

    except Exception as exc:
        logger.error("Failed to load source_system.order_items: {}", exc)
        raise


# ---------------------------------------------------------------------------
# Step 4 — Post-load validation
# ---------------------------------------------------------------------------

def run_validation() -> None:
    """Run row-count and FK-integrity validation queries and log results.

    Checks performed:
      - Row counts for all five enterprise tables
      - Orders with no matching customer (should be 0)
      - Order items with no matching order (should be 0)
    """
    engine = get_engine()

    count_queries: dict[str, str] = {
        "customers":   "SELECT COUNT(*) FROM source_system.customers",
        "stores":      "SELECT COUNT(*) FROM source_system.stores",
        "products":    "SELECT COUNT(*) FROM source_system.products",
        "orders":      "SELECT COUNT(*) FROM source_system.orders",
        "order_items": "SELECT COUNT(*) FROM source_system.order_items",
    }

    integrity_queries: dict[str, str] = {
        "orders_missing_customer_fk": (
            "SELECT COUNT(*) FROM source_system.orders o "
            "LEFT JOIN source_system.customers c ON o.customer_id = c.customer_id "
            "WHERE c.customer_id IS NULL"
        ),
        "order_items_missing_order_fk": (
            "SELECT COUNT(*) FROM source_system.order_items oi "
            "LEFT JOIN source_system.orders o ON oi.order_id = o.order_id "
            "WHERE o.order_id IS NULL"
        ),
    }

    logger.info("Running post-load validation...")

    with engine.connect() as conn:
        logger.info("--- Row counts ---")
        for label, sql in count_queries.items():
            try:
                count = conn.execute(text(sql)).scalar()
                logger.info("  source_system.{}: {:,} rows", label, count)
            except Exception as exc:
                logger.error("  Validation count query '{}' failed: {}", label, exc)

        logger.info("--- FK integrity checks ---")
        for label, sql in integrity_queries.items():
            try:
                count = conn.execute(text(sql)).scalar()
                if count == 0:
                    logger.info("  [PASS] {}: {} orphaned rows", label, count)
                else:
                    logger.warning("  [WARN] {}: {:,} orphaned rows detected", label, count)
            except Exception as exc:
                logger.error("  Validation FK query '{}' failed: {}", label, exc)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """Orchestrate the full Stage 1 source-system load end-to-end.

    Execution order:
      1. Download Olist CSVs from Kaggle (idempotent)
      2. Execute DDL to create enterprise schema objects (idempotent)
      3. Load customers  → build customer_lookup
      4. Load stores     → build store_lookup
      5. Load products   → build product_lookup
      6. Load orders     → build order_lookup  (requires customer_lookup)
      7. Load order_items                       (requires all three lookups)
      8. Run validation queries
    """
    logger.info("=== Stage 1: Source System Setup ===")

    # Step 1 — Download
    logger.info("Step 1/4 — Downloading Olist data")
    download_olist_data()

    # Step 2 — DDL
    logger.info("Step 2/4 — Creating enterprise schema objects")
    create_source_tables()

    # Step 3 — Load tables in FK-dependency order
    logger.info("Step 3/4 — Loading CSV data into source_system")

    customer_lookup = load_customers()
    logger.info("customer_lookup: {:,} entries", len(customer_lookup))

    store_lookup = load_stores()
    logger.info("store_lookup: {:,} entries", len(store_lookup))

    product_lookup = load_products()
    logger.info("product_lookup: {:,} entries", len(product_lookup))

    order_lookup = load_orders(customer_lookup)
    logger.info("order_lookup: {:,} entries", len(order_lookup))

    items_loaded = load_order_items(order_lookup, product_lookup, store_lookup)
    logger.info("order_items loaded: {:,} rows", items_loaded)

    # Step 4 — Validation
    logger.info("Step 4/4 — Post-load validation")
    run_validation()

    logger.info("=== Stage 1 complete ===")


if __name__ == "__main__":
    run()
