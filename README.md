# Multi-Source ETL Pipeline — Brazilian E-Commerce Analytics

A production-grade data engineering portfolio project integrating three independent data
sources — Kaggle's Olist Brazilian e-commerce dataset (99,441 orders / 112,650 order
line items), Open-Meteo historical weather (20 cities, ERA5), and Frankfurter FX rates
(daily USD/BRL) — into a unified PostgreSQL analytics warehouse via the medallion
architecture. Bronze preserves raw snapshots (Parquet for source-DB tables, JSON for
API responses); Silver enforces pandera schema
contracts with quarantine routing for invalid rows; Gold materialises a star schema
(3 facts, 5 dims) in both Parquet and PostgreSQL. The pipeline ships with Docker
Compose for one-command deployment and a pytest suite of 98 test functions (95 pure unit + 3 DB-integration skipped without a live DB) across a Python 3.10–3.12 CI matrix on GitHub Actions. A 4-page Power BI dashboard with 27 DAX measures is
specified in `docs/stage8–10*` against the `analytics` schema; the `.pbix` file is
authored separately (Power BI Desktop is Windows-only) and kept under `pbix/`.


---

## Business Questions Answered

- **Which Brazilian states generate the highest order revenue, and how do seasonal and regional factors influence purchasing patterns?** Order history is joined with geographic dimensions for state-level revenue analysis, category breakdowns, and time-series trending.
- **Does adverse weather correlate with order volume volatility or increased delivery failures?** Weather facts are joined daily to orders by city, enabling analysis of temperature, precipitation, and wind conditions against order placement and on-time delivery rates.
- **How does USD/BRL exchange rate volatility impact reported revenue when normalized to a standard currency?** FX rates are joined by order date to every order-item, enabling native-BRL reporting alongside USD-normalized comparisons.
- **Which product categories drive the highest freight costs relative to item value, and how does this vary by delivery region?** Line-item facts include unit price, freight cost, and product-to-geography foreign keys for margin and cost analysis.
- **How does delivery performance vary by seller, region, and product category?** `fact_sales` carries `delivery_days_actual` and `delivery_days_estimated` per line item, joinable to `dim_store` (seller geography) and `dim_product`. Customer-satisfaction analytics (review scores) are not yet supported — they land when `fact_reviews` ships (see Future Improvements).
- **Which sellers have the highest on-time delivery rates by state, and how do their performance metrics compare to peers?** Order-item facts include seller geography and delivery performance flags, enabling seller scorecard analysis.

---

## Architecture

### Pipeline Data Flow

```
┌──────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│  Olist CSVs      │  │  Open-Meteo API      │  │  Frankfurter API     │
│  (Kaggle)        │  │  (no key required)   │  │  (no key required)   │
└────────┬─────────┘  └──────────┬───────────┘  └──────────┬───────────┘
         │                       │                          │
         └───────────────────────┴──────────────────────────┘
                                            │
                                            ▼
                              ┌─────────────────────────────────────────┐
                              │               BRONZE LAYER               │
                              │                                          │
                              │  data/bronze/                            │
                              │  PostgreSQL: source_system schema        │
                              │                                          │
                              │  • DB snapshot, 5 modelled tables        │
                              │      (Parquet under bronze/db/)          │
                              │  • Raw Olist snapshots, 4 unmodelled     │
                              │      (payments, reviews, geolocation,    │
                              │       category_translation — Parquet)    │
                              │  • Weather records, JSON via Open-Meteo  │
                              │  • FX rates, JSON via Frankfurter        │
                              └─────────────────┬───────────────────────┘
                                                │
                                                ▼
                              ┌─────────────────────────────────────────┐
                              │               SILVER LAYER               │
                              │                                          │
                              │  data/silver/          (Parquet)         │
                              │  data/silver/quarantine/  (rejected rows)│
                              │                                          │
                              │  • Type-cast and null-checked            │
                              │  • Deduplicated; pandera schema enforced │
                              │  • Invalid rows isolated to quarantine/  │
                              └─────────────────┬───────────────────────┘
                                                │
                                                ▼
                              ┌─────────────────────────────────────────┐
                              │               GOLD LAYER                 │
                              │                                          │
                              │  data/gold/            (Parquet)         │
                              │  PostgreSQL: analytics schema            │
                              │                                          │
                              │  Dimensions (5)                          │
                              │    dim_date · dim_customer               │
                              │    dim_product · dim_store               │
                              │    dim_currency                          │
                              │                                          │
                              │  Facts (3)                               │
                              │    fact_sales (grain: order line item)   │
                              │    fact_weather_daily (grain: city+date) │
                              │    fact_fx_rates (grain: date+ccy pair)  │
                              └─────────────────┬───────────────────────┘
                                                │
                                                ▼
                              ┌─────────────────────────────────────────┐
                              │            CONSUMPTION LAYER             │
                              │                                          │
                              │  Power BI Desktop (Import mode)          │
                              │  4 report pages · 27 DAX measures        │
                              │  Designed in docs/stage8–10*; .pbix      │
                              │  authored in a Windows VM and saved to   │
                              │  pbix/ (git-ignored). See                │
                              │  docs/powerbi_vm_workflow.md.            │
                              └─────────────────────────────────────────┘
```

### Gold Layer — Star Schema

`fact_sales` is the primary fact table. `fact_weather_daily` and `fact_fx_rates` are
independent conformed facts that share `dim_date` and `dim_currency`.

```
                         ┌──────────────────┐
                         │    dim_date       │
                         │──────────────────│
                         │ date_key INT PK   │◄─────────────────────────┐
                         │   (YYYYMMDD)      │◄──────────┐              │
                         └──────────────────┘           │              │
                                                        │              │
┌─────────────────┐   ┌────────────────────────────────┐│  ┌───────────────────────────┐
│  dim_customer   │   │          fact_sales             ││  │   fact_weather_daily      │
│─────────────────│   │────────────────────────────────││  │───────────────────────────│
│ customer_key PK │◄──│ customer_key     FK             ││  │ date_key      FK ─────────┘
└─────────────────┘   │ product_key      FK             ││  │ city                      │
                      │ store_key        FK             ││  │ state                     │
┌─────────────────┐   │ currency_key     FK             │└──│ temp_max                  │
│  dim_product    │   │ date_key         FK ────────────┘   │ temp_min                  │
│─────────────────│   │                                     │ precipitation             │
│ product_key  PK │◄──│ order_item_id    PK                 │ windspeed                 │
└─────────────────┘   │ Grain: order line item              │ weathercode               │
                      │ ~112,650 rows                       └───────────────────────────┘
┌─────────────────┐   └────────────────────────────────┐
│   dim_store     │                                    │   ┌───────────────────────────┐
│─────────────────│                                    │   │      fact_fx_rates         │
│ store_key    PK │◄──── fact_sales.store_key          │   │───────────────────────────│
└─────────────────┘                                    │   │ date_key      FK ──────────┘
                                                        │   │ base_currency_key  FK      │
┌─────────────────┐                                    │   │ quote_currency_key FK      │
│  dim_currency   │                                    │   │ base_currency              │
│─────────────────│                                    │   │ quote_currency             │
│ currency_key PK │◄──── fact_sales.currency_key       │   │ rate                       │
│                 │◄──── fact_fx_rates.base/quote_key  │   └───────────────────────────┘
└─────────────────┘
```

> **`dim_date` key:** `date_key` is an `INT` in `YYYYMMDD` format (e.g. `20171125`),
> not a native `DATE` type. All three fact tables join to `dim_date` on this integer key.

### Orchestration Modes

| Mode | Flag | Stages executed | When to use |
|------|------|-----------------|-------------|
| **Full refresh** | `--full-refresh` | `extract → silver → gold → warehouse → quality` | First run or when source data has changed |
| **Incremental** | `--incremental` | `silver → gold → warehouse → quality` | Bronze Parquet is current; skip re-extraction |
| **Single stage** | `--stage <name>` | Exactly one named stage | Debugging or re-running a specific step |

Available `--stage` names: `init`, `setup`, `extract`, `load`, `silver`, `gold`, `warehouse`, `quality`.

Pass `--no-fail-fast` to continue executing remaining stages after a failure instead of
halting immediately.

---

## Tech Stack

| Component | Technology | Version | Rationale |
|-----------|------------|---------|-----------|
| Runtime | Python | 3.10+ | Strong data libraries, async/sync interop, broad DevOps tooling support |
| Data frames | pandas | 2.x | Columnar memory model, rich groupby/pivot operations, Parquet I/O via pyarrow |
| Columnar storage | pyarrow | 14.0+ | Parquet read/write, zero-copy interchange, efficient compression |
| Spreadsheet read | openpyxl | 3.1+ | Drop-in for pandas Excel I/O without xlrd/xlwt legacy quirks |
| Database ORM | SQLAlchemy | 2.x | Composable Core + ORM hybrid, PEP 249 compliance, safe parameter binding |
| PostgreSQL driver | psycopg2-binary | 2.9+ | Mature, widely audited, fast bulk inserts via COPY protocol |
| HTTP (REST APIs) | requests | 2.31+ | Ubiquitous, simple request/response model for one-shot endpoints |
| HTTP (HTTP/2) | httpx | 0.27+ | Async-first, connection pooling, parity with requests API |
| Kaggle client | kagglehub | 0.2+ | Official Kaggle client, automatic credential detection, cache layer |
| Fuzzy matching | rapidfuzz | 3.6+ | 10–100× faster than fuzzywuzzy; vectorized Rust/C backend for city-name joins |
| Accent stripping | unicodedata2 | 15.1+ | Full Unicode 15.1 support for accent normalization before fuzzy join |
| Data contracts | pydantic | 2.5+ | Schema contracts with field-level validators, strict mode, runtime type checks |
| DataFrame validation | pandera | 0.18+ | Pandas-native schema validation at the Silver boundary; declarative checks |
| Retry logic | tenacity | 8.2+ | Clean decorator API, exponential backoff with jitter, predicate-based retry filters |
| Logging | loguru | 0.7+ | Lazy string interpolation, structured log sinks, automatic rotation and compression |
| Progress bars | tqdm | 4.66+ | Minimal overhead, ETA calculation, integration with pandas iterators |
| Secrets management | python-dotenv | 1.0+ | Prevents hardcoded credentials in version control; `.env` git-ignored by default |

---

## Data Sources

| Source | Format | Volume | Auth |
|--------|--------|--------|------|
| Olist Brazilian E-Commerce (Kaggle) | 9 CSV files | ~99,441 orders / 112,650 items | Kaggle login (free) |
| Open-Meteo Archive API | JSON (REST) | ~730 days × 20 cities | None |
| Frankfurter FX API | JSON (REST) | ~550 trading days, direct USD/BRL | None |

---

## Quick Start

Both options require **Docker Desktop** (or Docker Engine + Compose v2) for PostgreSQL.
Choose the run mode that suits you:

| | Option A — Local Python | Option B — Docker (no Python install) |
|---|---|---|
| **Requires** | Python 3.10+, Docker | Docker only |
| **Run command** | `make full-refresh` | `make docker-run` |
| **Best for** | Development, debugging | Reproducible / CI runs |

### Step 1 — Clone and configure (both options)

```bash
git clone <repo-url> "Multi-Source ETL"
cd "Multi-Source ETL"

cp .env.example .env
# Edit .env — fill in three values:
#   DB_PASSWORD    your chosen Postgres password
#   KAGGLE_USERNAME  your Kaggle account name
#   KAGGLE_KEY       your Kaggle API key (kaggle.com/settings → API)
```

### Step 2A — Local Python

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
make install                     # pip install -r requirements.txt

make db-up                       # start PostgreSQL in Docker
make db-status                   # wait until etl-postgres shows "healthy"
make smoke-test                  # verify DB connectivity

make bootstrap                   # init → setup → full-refresh in one command
                                 # (init/setup are idempotent on subsequent runs)
make logs                        # tail -f logs/etl.log
```

For day-to-day re-runs after the first bootstrap:

```bash
make full-refresh                # re-extract from APIs/DB then re-transform
make incremental                 # skip extract; re-transform from existing Bronze
```

### Step 2B — Fully containerised

```bash
make docker-build                # build the etl-pipeline image

make db-up                       # start PostgreSQL
make db-status                   # wait until etl-postgres shows "healthy"

docker compose run --rm etl-pipeline --stage init    # create schemas
docker compose run --rm etl-pipeline --stage setup   # load source data

make docker-run                  # run the full pipeline
make logs                        # check logs/etl.log on the host
make db-down                     # stop Postgres when done (data is preserved)
```

> **DB_HOST note:** `.env` keeps `DB_HOST=localhost` for local runs. When using
> `docker compose run`, the Compose file automatically overrides it to `etl-postgres`.
> No manual change is needed when switching between options.

---

## Setup & Configuration

### Environment Variables (`.env`)

Copy `.env.example` to `.env` and fill in the required values. The file is git-ignored.

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `DB_HOST` | `localhost` | Yes | PostgreSQL host |
| `DB_PORT` | `5432` | Yes | PostgreSQL port |
| `DB_NAME` | `etl_pipeline` | Yes | Database name |
| `DB_USER` | `postgres` | Yes | Database user |
| `DB_PASSWORD` | — | **Yes** | Database password |
| `KAGGLE_USERNAME` | — | **Yes** | Kaggle account username |
| `KAGGLE_KEY` | — | **Yes** | Kaggle API key |
| `WEATHER_PROVIDER` | `open-meteo` | No | Weather API provider |
| `FX_PROVIDER` | `frankfurter` | No | FX rate provider |
| `PIPELINE_START_DATE` | `2016-09-01` | No | Coverage start date |
| `PIPELINE_END_DATE` | `2018-10-31` | No | Coverage end date |
| `WEATHER_CITY_COUNT` | `20` | No | Top N cities to fetch weather for |
| `FX_BASE_CURRENCY` | `USD` | No | FX base currency |
| `FX_QUOTE_CURRENCY` | `BRL` | No | FX quote currency |
| `LOG_LEVEL` | `INFO` | No | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

### Kaggle Credentials

Get your API token from [kaggle.com/settings → API → Create New Token](https://www.kaggle.com/settings).

```bash
# macOS / Linux — place kaggle.json at the expected location
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

Alternatively, set `KAGGLE_USERNAME` and `KAGGLE_KEY` directly in `.env`.

### Docker (PostgreSQL)

```bash
make db-up        # Start PostgreSQL (first run mounts 00_init.sql automatically)
make db-status    # Verify health — etl-postgres should show "healthy"
make db-shell     # Open interactive psql session
make db-reset     # Wipe all data and start fresh (destroys etl_pgdata volume)
```

**First-run note:** `00_init.sql` is mounted into `/docker-entrypoint-initdb.d/` and
runs automatically only when the named volume `etl_pgdata` is empty. It creates both
schemas (`source_system`, `analytics`) and the `pipeline_metadata` table.

---

## Running the Pipeline

### Full pipeline (recommended first run)

```bash
python main.py --full-refresh
# Equivalent: make full-refresh
```

### Incremental (Bronze already current)

```bash
python main.py --incremental
# Equivalent: make incremental
```

### Single stage

```bash
python main.py --stage extract     # Stage 1 only
python main.py --stage silver      # Stage 3 only
python main.py --stage warehouse   # Stage 5 only
python main.py --stage quality     # Stage 7 only
```

### All CLI options

```
python main.py [--full-refresh | --incremental | --stage STAGE] [--no-fail-fast]

  --full-refresh     Re-extract all data from APIs/DB, then transform and load.
  --incremental      Skip extraction; re-run Silver → Gold → Warehouse → Quality only.
  --stage STAGE      Run exactly one stage. Choices:
                       init, setup, extract, load, silver, gold, warehouse, quality
  --no-fail-fast     Continue on failure rather than stopping at the first error.
```

---

## Project Structure

```
Multi-Source ETL/
├── main.py                          # Pipeline orchestrator (argparse CLI)
├── Makefile                         # Helper commands for all pipeline operations
├── pyproject.toml                   # Tool config: black, ruff, mypy, pytest
├── requirements.txt                 # Runtime Python dependencies
├── requirements-dev.txt             # Dev tools: pytest, black, ruff, mypy, jupyter
├── Dockerfile                        # Multi-stage image: deps layer + slim runtime
├── .dockerignore                    # Excludes .env, data/, logs/, tests/ from build context
├── docker-compose.yml               # PostgreSQL 16 + etl-pipeline app service (profiles)
├── docker-compose.override.yml      # Dev: pgAdmin on :5050 (auto-merged locally)
├── .env.example                     # Credentials template — copy to .env, never commit
├── .gitignore                       # Excludes .env, /data/, *.pbix, __pycache__
│
├── scripts/
│   └── test_db_connection.py        # Stage 0 smoke test — 6 connectivity checks
│
├── sql/
│   ├── ddl/
│   │   ├── 00_init.sql              # Docker entrypoint: schemas + pipeline_metadata
│   │   ├── 01_schemas.sql           # Schema stubs (idempotent)
│   │   ├── 02_pipeline_metadata.sql # Metadata table DDL (idempotent)
│   │   ├── 03_source_system.sql     # source_system schema: 5 modelled source tables (other 4 Olist CSVs are Bronze-only)
│   │   ├── 04_gold_schema.sql       # analytics schema: 5 dims + 3 facts + indexes
│   │   ├── 05_data_quality.sql      # data_quality_log table DDL
│   │   └── 06_powerbi_readiness.sql # powerbi_reader role, extra indexes, v_sales_enriched view
│   └── queries/
│       ├── check_connection.sql     # One-shot connectivity verification
│       └── row_counts.sql           # Row counts across all ETL tables
│
├── src/
│   ├── setup/
│   │   └── load_source_db.py        # Stage 0b: Kaggle download + schema + CSV load
│   ├── extract/
│   │   ├── extract_db.py            # Stage 1a: source_system → Bronze Parquet snapshot
│   │   ├── extract_api.py           # Stage 1b: API orchestrator (weather + FX)
│   │   ├── extract_olist_csvs.py    # Stage 1c: snapshot the 4 unmodelled raw Olist CSVs to Bronze
│   │   ├── extract_weather.py       # Open-Meteo API: ERA5 historical, retry + cache
│   │   └── extract_fx.py            # Frankfurter API: daily FX rates, gap-fill
│   ├── transform/
│   │   ├── transform_sales.py       # Stage 3a: orders + order_items → Silver Parquet
│   │   ├── transform_weather.py     # Stage 3b: weather → Silver Parquet
│   │   ├── transform_fx.py          # Stage 3c: FX rates → Silver Parquet (ffill gaps)
│   │   ├── build_dimensions.py      # Stage 4a: 5 Gold dimension tables
│   │   ├── build_facts.py           # Stage 4b: 3 Gold fact tables
│   │   ├── schemas.py               # Pandera schema contracts (Silver boundary)
│   │   ├── gold_utils.py            # Shared helpers: read_latest_silver, write_gold
│   │   └── utils.py                 # Transform utilities
│   ├── load/
│   │   └── load_to_warehouse.py     # Stage 5: Gold Parquet → PostgreSQL analytics schema
│   ├── orchestration/
│   │   └── pipeline.py              # PipelineConfig, PipelineMode, run_pipeline()
│   ├── quality/
│   │   ├── checks.py                # 6 reusable check primitives + table-specific suites
│   │   └── runner.py                # Stage 7 orchestrator: run → persist → halt
│   └── utils/
│       ├── db.py                    # get_engine(), get_connection(), init_schemas()
│       ├── logger.py                # loguru: stdout INFO + rotating file DEBUG
│       └── validators.py            # normalize_city_name(), validate_dataframe()
│
├── data/                            # Excluded from git (tracked via .gitkeep)
│   ├── bronze/                      # Raw snapshots (olist/, weather/, fx/, manual/)
│   ├── silver/                      # Cleaned Parquet (sales/, weather/, fx/, quarantine/)
│   └── gold/                        # Star schema Parquet (dimensions/, facts/)
│
├── docs/
│   ├── source_schema.md             # source_system ER diagram + 8 data quality gotchas
│   ├── stage8_powerbi.md            # Power BI connection plan + semantic model design
│   ├── stage9_dax_measures.md       # 27 DAX measures with format strings + rationale
│   ├── stage10_dashboard_pages.md   # 4-page dashboard design spec + accessibility guide
│   ├── POWER_BI_SEMANTIC_MODEL_DESIGN.md
│   ├── powerbi_vm_workflow.md       # Power BI Desktop on macOS via Parallels VM
│   └── screenshots/                 # PNG exports of each dashboard page
│
├── pbix/                            # Power BI Desktop files (excluded from git)
├── notebooks/                       # Jupyter exploration notebooks
│   └── 01_eda_sales.ipynb           # Sales EDA against the analytics schema
├── logs/                            # Rotating etl.log (excluded from git)
└── tests/
    ├── conftest.py                  # Shared pytest fixtures (minimal_orders_df, minimal_fx_df, minimal_weather_df)
    ├── test_transforms.py           # Silver transform logic: orders, weather, FX (14 tests)
    ├── test_validators.py           # normalize_city_name, validate_dataframe, DQ report (16 tests)
    ├── test_gold_utils.py           # assign_surrogate_keys, check_referential_integrity (12 tests)
    ├── test_silver_utils.py         # write_silver, quarantine_rows, read_latest_bronze_parquet (19 tests)
    ├── test_schemas.py              # Pandera schema validation via validate_silver (22 tests)
    ├── test_transform_functions.py  # Full transform functions with mocked I/O (12 tests)
    └── test_db_connection.py        # PostgreSQL connectivity checks (3 tests; skipped if no DB)
```

---

## Skills Demonstrated

| Skill | Where | What It Demonstrates |
|-------|-------|----------------------|
| Multi-source ingestion | `src/extract/extract_db.py`, `extract_api.py`, `extract_olist_csvs.py`, `extract_weather.py`, `extract_fx.py` | Pulls from Kaggle CSV (via `setup`) into the `source_system` schema, snapshots both modelled and unmodelled Olist CSVs to Bronze Parquet, and fetches Open-Meteo + Frankfurter REST APIs |
| Medallion architecture | `src/orchestration/pipeline.py`, `data/bronze → silver → gold` | Three-layer progression: raw → cleaned → modeled; each layer has explicit quality gates and schema contracts |
| Dimensional modelling | `src/transform/build_dimensions.py`, `build_facts.py` | Five dimension tables + three fact tables with surrogate keys, grain documentation, and referential integrity checks |
| API resilience / retry | `src/extract/extract_weather.py` | Hand-rolled bounded-retry loop with exponential backoff for transient HTTP failures on the Open-Meteo extractor; the FX extractor relies on Frankfurter's stable public endpoint and does not retry. |
| SQL injection prevention | `src/quality/checks.py` lines 29–63 | Allowlist (`_ANALYTICS_TABLES` frozenset) validates every dynamic table name; parameterized queries for all values |
| Data quality automation | `src/quality/checks.py`, `src/quality/runner.py` | Six reusable check primitives (row_count, null, uniqueness, range, referential integrity, column comparison); results persisted to `analytics.data_quality_log` |
| Incremental load design | `src/orchestration/pipeline.py` (`INCREMENTAL_STAGES`), `src/load/load_to_warehouse.py` | Bronze layer skipped; pipeline re-runs Silver → Gold → Warehouse → Quality. Idempotency: dimensions reload via TRUNCATE+INSERT; facts upsert via `INSERT … ON CONFLICT (pk_cols) DO UPDATE` so re-runs never produce duplicate rows. `_loaded_at` is a batch-stamp audit column, not the dedup mechanism. |
| Pipeline orchestration | `src/orchestration/pipeline.py` | Three modes (FULL_REFRESH, INCREMENTAL, SINGLE); fail_fast flag; callable stage registry; Prefect upgrade path documented in module docstring |
| City name normalisation | `src/utils/validators.py::normalize_city_name()` | NFD decomposition strips accents (stdlib `unicodedata`), lowercases, trims whitespace. Used by `src/transform/transform_weather.py` on weather observation cities and by `src/transform/build_dimensions.py::build_dim_customer` to populate `dim_customer.normalized_city`, so `analytics.v_sales_with_weather` joins exactly without a Postgres `unaccent()` extension. |
| FX rate ingestion | `src/extract/extract_fx.py`, `src/transform/transform_fx.py` | Fetches the direct USD→BRL daily series from Frankfurter, forward-fills weekend/holiday gaps so every calendar day has a rate, and persists to `analytics.fact_fx_rates`. Pre-joined to sales as `analytics.v_sales_usd` (`unit_price_usd = unit_price_brl / rate`). |
| Structured logging | `src/utils/logger.py` | loguru with dual sinks: INFO+ to stdout (colored), DEBUG+ to rotating file (10 MB, 7-day retention); lazy string interpolation |
| Power BI model + DAX (designed) | `docs/stage9_dax_measures.md`, `docs/stage10_dashboard_pages.md` | 27 DAX measures specified across 6 display folders; Import-mode star-schema design with role-playing currency dimension; 4-page dashboard layout with accessibility spec. The `.pbix` is authored separately on a Windows VM (see `docs/powerbi_vm_workflow.md`); screenshots in `docs/screenshots/` when exported. |
| Containerisation | `Dockerfile`, `docker-compose.yml` | Multi-stage Docker build (deps layer + slim runtime, non-root user); Compose profiles so `db-up` starts only Postgres and `docker compose run` opts in the pipeline; DB_HOST override pattern eliminates `.env` edits between modes |
| Testing & CI | `tests/`, `.github/workflows/ci.yml` | 98 test functions (95 pure unit + 3 DB-integration skipped without a live DB) covering transform logic, schema contracts, utility I/O, and Gold helpers; pandera and mocked I/O boundaries; pytest-cov with 60% branch-coverage gate; GitHub Actions matrix on Python 3.10–3.12 with ruff, black, mypy, and Codecov |

---

## Pipeline Stage Reference

| `--stage` value | Description |
|-----------------|-------------|
| `init` | Create PostgreSQL schemas and `pipeline_metadata` table |
| `setup` | Download Olist CSVs from Kaggle, create `source_system` schema, load the 5 modelled source tables (`customers`, `stores`, `products`, `orders`, `order_items`). The other 4 raw CSVs are snapshotted to Bronze in stage `extract` (1d). |
| `extract` | Stage 1a: snapshot `source_system` to Bronze Parquet (5 tables). Stage 1b: pull Open-Meteo weather (20 cities) and Frankfurter FX rates. Stage 1c: snapshot the 4 unmodelled raw Olist CSVs (payments / reviews / geolocation / category translation) to Bronze Parquet. |
| `silver` | Transform Bronze → Silver: type-cast, deduplicate, validate with pandera, quarantine invalid rows |
| `gold` | Build 5 Gold dimension tables and 3 Gold fact tables from Silver Parquet |
| `warehouse` | Load Gold Parquet into PostgreSQL `analytics` schema (dims: truncate+reload; facts: upsert) |
| `quality` | Run automated quality checks against `analytics.*` tables; persist results to `data_quality_log` |

---

## Testing & CI

![CI](https://github.com/ASDFGHJKLZXC123/Multi-Source-ETL/actions/workflows/ci.yml/badge.svg)

### Running tests locally

```bash
# Install dev dependencies (includes pytest, pytest-cov, pytest-mock)
make install-dev

# Run the full test suite with coverage
make test
# Equivalent: pytest tests/ -v --cov=src/transform --cov-report=term-missing

# Run without coverage (faster during development)
pytest tests/ -v --no-cov

# Run a single test file
pytest tests/test_transform_functions.py -v

# Run a single test
pytest tests/test_transforms.py::TestTransformFx::test_forward_fill_missing_dates -v
```

### Test suite structure

| File | Scope | Tests |
|------|-------|-------|
| `tests/test_transforms.py` | Silver transform logic — orders, weather, FX | 14 |
| `tests/test_validators.py` | `normalize_city_name`, `validate_dataframe`, DQ report | 16 |
| `tests/test_gold_utils.py` | `assign_surrogate_keys`, `check_referential_integrity` | 12 |
| `tests/test_silver_utils.py` | `write_silver`, `quarantine_rows`, `read_latest_bronze_parquet`, `log_transform_summary` | 19 |
| `tests/test_schemas.py` | Pandera schema validation via `validate_silver` | 22 |
| `tests/test_transform_functions.py` | Full transform functions with mocked I/O | 12 |
| `tests/test_db_connection.py` | PostgreSQL connectivity (skipped without a live DB) | 3 |

**Total: 98 test functions — 95 pure unit + 3 DB-integration skipped without a live DB.** Unit tests require no database, network, or Parquet files on disk.

### Coverage target

Coverage is measured on `src/transform/` only. The `--cov-fail-under=60` flag causes `pytest` to exit with code 1 if coverage drops below 60%, enforcing the gate in CI. Branch coverage is enabled (`branch = true`) to test both sides of quarantine guards.

### CI/CD

`.github/workflows/ci.yml` runs on every push and pull request to `main`.

| Step | Tool | Fails build? |
|------|------|-------------|
| Lint | ruff | Yes |
| Format check | black | Yes |
| Type check | mypy | No (continue-on-error) |
| Unit tests + coverage | pytest-cov | Yes (below 60%) |
| Coverage upload | Codecov | No (optional) |

Matrix: Python 3.10, 3.11, 3.12 on `ubuntu-latest`.

---

## Data Quality Framework

Quality checks run after warehouse load (Stage 7) and log results to
`analytics.data_quality_log`. Checks cover all three fact tables.

**Check types:**

| Check | What It Tests |
|-------|--------------|
| Row count threshold | Minimum expected rows in each fact/dim table |
| Null rate | Columns that must not exceed a null fraction |
| Uniqueness | Primary key columns must contain no duplicates |
| Value range | Numeric columns must fall within expected bounds |
| Referential integrity | Fact FK columns must resolve to valid dimension PKs |
| Column comparison | e.g., `delivery_days_actual >= 0` |

**Severity levels:** `INFO` (informational) → `WARNING` (investigate) → `CRITICAL` (halt).

**Configurable halt threshold:**
```bash
python main.py --stage quality           # halts on CRITICAL (default)
python -m src.quality.runner --halt-on WARNING   # halts on WARNING or above
```

**SQL injection protection:** All check functions validate table names against
`_ANALYTICS_TABLES` (a `frozenset`) before interpolating into SQL. Any name not in
the allowlist raises `ValueError` immediately.

---

## Power BI Integration

See [`docs/stage8_powerbi.md`](docs/stage8_powerbi.md) for the full connection guide and semantic model design.

**Quick reference:**

| Item | Value |
|------|-------|
| Connection mode | Import (not DirectQuery) |
| Driver | Npgsql 6.0.x |
| Schema | `analytics` (set via `options=-csearch_path=analytics` in Advanced Options) |
| Role | `powerbi_reader` (SELECT-only, created by `06_powerbi_readiness.sql`) |
| date_key conversion | `#date(Number.IntegerDivide([date_key],10000), ...)` in Power Query |
| Role-playing currency | Duplicate Currencies query as "Currencies (Quote)" for FX base/quote FKs |
| DAX measures | 27 measures documented in [`docs/stage9_dax_measures.md`](docs/stage9_dax_measures.md) |
| Dashboard | 4 pages documented in [`docs/stage10_dashboard_pages.md`](docs/stage10_dashboard_pages.md) |

**`.pbix` files** are stored in `pbix/` (git-ignored). **Exported PNG screenshots**
of each dashboard page should be placed under `docs/screenshots/` once the `.pbix`
is built and exported (the directory currently holds only its README). For the
macOS authoring workflow (Power BI Desktop is Windows-only), see
[`docs/powerbi_vm_workflow.md`](docs/powerbi_vm_workflow.md).

---

## Known Limitations

- **2016 data sparsity**: Orders begin in Q4 2016; the first three months contain only ~3% of annual volume. Year-over-year comparisons are misleading without explicit filtering to 2017–2018.
- **Weather coverage limited to 20 cities (~61% miss)**: The Open-Meteo extractor pulls daily observations for the top 20 cities by order volume only; sales from any other city land in `fact_sales` with no matching `fact_weather_daily` row (`v_sales_with_weather` shows ~38.7% match rate). Increasing coverage means raising `WEATHER_CITY_COUNT` in `.env` and re-running extract.
- **FX is mid-market USD/BRL only**: `fact_fx_rates` carries one row per calendar day for the direct USD→BRL pair from Frankfurter, weekend/holiday-gap forward-filled. Rates are mid-market and not suitable for transaction-grade financial reporting.
- **Item-grain delivery metrics**: Multi-item orders appear once per line-item in `fact_sales`. A five-item order shipped one week late counts as five late items. Order-level SLA analysis requires aggregation back to `order_id` grain.
- **Review data is Bronze-only, no Silver/Gold layer**: Customer review data is snapshotted to `data/bronze/db/reviews/snapshot.parquet` (Stage 1c) but no Silver transform or Gold fact table exists yet. Source data has duplicate `review_id` values and inconsistent timestamps; promoting it to a `fact_reviews` table is on the roadmap (see Future Improvements).
- **Brasília time in source timestamps**: All Olist timestamps are in Brasília time (UTC-3) without explicit timezone metadata. Power BI and SQL queries assume UTC-3; explicit offset application is required for other time zones.
- **Import mode Power BI**: The dashboard refreshes by re-importing from PostgreSQL, not via DirectQuery. Refresh requires re-running the pipeline on the source machine.

---

## Future Improvements / Explicit Roadmap

The four raw Olist CSVs not modelled today (payments, reviews, geolocation,
category_translation) are now snapshotted to Bronze Parquet on every extract
run, but no Silver or Gold layer is built from them. Promoting them is the
next planned increment:

- **`fact_payments`**: grain = one row per payment record (~100k rows). FKs
  to `dim_date` (via `order_id` join) and a degenerate `order_code`. Measures:
  `payment_value`, `payment_installments`, `payment_sequential`. Attribute:
  `payment_type` (credit_card / boleto / voucher / debit_card). Implementation
  scope: source_system DDL + load function + pandera schema + Silver transform
  + Gold builder + warehouse loader + quality checks (~1 day).
- **`fact_reviews`**: grain = one row per review (~100k rows). FKs to
  `dim_date` (review creation) and `order_code`. Measures: `review_score`,
  comment length. Currently flagged in Known Limitations because of source
  data quality issues (missing reviews, inconsistent timestamps); promoting
  it requires deciding the quarantine policy first.
- **`dim_geolocation`**: enrich `dim_customer` and `dim_store` with
  latitude/longitude from the geolocation snapshot, unlocking heatmap and
  distance-based analytics.

Other improvements:

- **Prefect or Airflow orchestration**: `pipeline.py` is structured for easy migration — its module docstring maps `PipelineConfig → @flow parameters`, `run_pipeline() → @flow`, and `_execute_stage() → @task`. Converting requires only decorator additions.
- **Incremental weather and FX extraction**: Both APIs currently re-pull the full date range on every extract run. Date-range partitioning with check-if-exists guards would reduce API load and runtime significantly.
- **dbt for Silver/Gold transforms**: Current pandas transforms are functional but lack SQL-based lineage and test visibility. Migrating to dbt would enable automated doc generation and tighter integration with a modern data warehouse.
- **DirectQuery mode for Power BI**: Provisioning a production PostgreSQL instance with read replicas would allow Power BI DirectQuery, enabling real-time dashboards without a re-import refresh cycle.

---

## Development

```bash
# Install dev dependencies
make install-dev

# Format code (black)
make format

# Lint (ruff)
make lint

# Type check (mypy)
make typecheck

# Run tests
make test

# Remove bytecode and cache files
make clean
```

Code style: Black (100-char line length), ruff, mypy strict. Configuration in `pyproject.toml`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `EnvironmentError: Missing DB_HOST` | `.env` not configured | `cp .env.example .env` and fill in values |
| `psycopg2.OperationalError: connection refused` | PostgreSQL not running | `make db-up && make db-status` |
| `No Parquet files found in Bronze` | Extract not run yet | `make extract` first |
| Silver quarantine has unexpected rows | Data quality issue in source | Check `data/silver/quarantine/` for rejection reasons |
| `analytics.data_quality_log` has CRITICAL rows | Quality gate triggered | Inspect log, then re-run `make full-refresh` |
| Power BI "Column not found" error | Stale semantic model | Refresh all queries in Power Query Editor |
| `schemas are missing` error at startup | `00_init.sql` not run | `make init` |

---

## Attribution

- **Olist Brazilian E-Commerce Public Dataset** — Kaggle (CC BY 4.0)
- **Open-Meteo Historical Weather API** — free, no key required, ERA5 data
- **Frankfurter Exchange Rate API** — free, no key required, ECB data

---

*A new contributor can get the project running end-to-end by following the [Quick Start](#quick-start) section above.*
