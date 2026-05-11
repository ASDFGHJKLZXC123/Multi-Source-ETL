# Stage 14 — Resume & Interview Prep

This document packages the project for resume use and interview preparation.
All claims are grounded in the actual implementation — nothing here is aspirational.

---

## Resume Bullet

> Engineered a Python medallion ETL pipeline integrating 3 independent data sources (Olist e-commerce, Open-Meteo weather, Frankfurter FX) into a PostgreSQL analytics warehouse, processing 112,650 order line items through Bronze→Silver→Gold stages with pandera schema validation, automated data quality checks, and a 27-measure Power BI dashboard; tested with 85 unit tests across a Python 3.10–3.12 CI matrix on GitHub Actions.

**Why it works:** Quantifies scope (3 sources, 112,650 rows, 27 measures, 85 tests), names specific technologies, and describes the outcome (analytics-ready warehouse + dashboard). Fits in two lines.

---

## Project Summary

This project solves the analytical gap that arises when e-commerce operations data lives in isolation from the external context that explains its behaviour. By consolidating three heterogeneous feeds — Olist Brazilian e-commerce transactions (99k orders), Open-Meteo historical weather (daily observations for 20 cities), and Frankfurter FX rates (daily USD/BRL) — into a single unified warehouse, the pipeline makes cross-domain questions answerable that were previously blocked by schema and granularity mismatches. The architecture follows the medallion pattern: Bronze preserves raw Parquet snapshots and source tables; Silver enforces pandera schemas and routes rejected rows to a quarantine layer with explicit rejection reasons; Gold materializes a star schema (3 facts, 5 dims) in both Parquet and PostgreSQL. The output is a production-grade analytics foundation backing a 4-page Power BI dashboard with 27 DAX measures, enabling revenue analysis by geography, weather-to-delivery correlation, and multi-currency normalization.

---

## Business Value Framing

### The problem
E-commerce operations generate transactional data that, in isolation, explains what happened but not why. When order volume drops in a region, a sales database alone cannot distinguish between a weather disruption, a currency shock making imported goods more expensive, or a fulfillment breakdown — because those explanations live in entirely separate systems with different schemas, update frequencies, and granularities. Joining a 99k-order relational dataset to a daily weather time series across 20 cities and a daily FX rate series requires non-trivial temporal alignment, geographic mapping, and currency normalization before a single analytical question can be answered reliably.

### The value delivered
This pipeline makes six categories of business decision tractable that were previously blocked by data silos. A logistics manager can see whether delivery failure rates in specific states spike on days with recorded adverse weather — the difference between blaming the carrier and adjusting routing policies. A finance team can compare USD-normalized revenue against raw BRL totals on the same dashboard, exposing whether apparent revenue growth during BRL depreciation was real or a currency artefact. A category manager can rank product lines by freight cost as a percentage of item value by region, directly informing pricing and warehouse placement. None of these analyses required new data collection — they required the integration work the pipeline performs.

### Why this pattern transfers
The medallion architecture and multi-source star schema here map directly to production data engineering patterns: a retail bank combining transaction ledgers with credit bureau feeds and macroeconomic indicators for loan risk scoring; a SaaS company merging product usage events with CRM deal data and support tickets to build a customer health score; a supply chain team joining ERP shipment records with carrier API feeds and commodity price indexes to forecast procurement cost. In each case, the critical design decision is the same: separate raw integration concerns from business-rule-heavy transformation, and land everything in a dimensional model that analysts can query without understanding source systems.

---

## Interview Q&A

### Q1: Walk me through the pipeline architecture.

Data flows through three entry points — the Olist dataset (9 CSVs, ~99k orders), Open-Meteo API (daily weather for top 20 cities by order volume), and Frankfurter API (daily FX rates) — into the Bronze layer, where raw Parquet snapshots are persisted alongside a PostgreSQL `source_system` schema replicating each source's table structure. The Silver layer applies pandera schemas in `lazy=True` mode, collecting all violations before raising so a single row can be tagged with multiple rejection reasons; rejected rows go to a `quarantine/` directory with `quarantine_reason` and `quarantined_at` columns. The Gold layer materializes a star schema across three facts (fact_sales at order line-item grain, fact_weather_daily at city+date, fact_fx_rates at date+currency pair) and five dimensions, written to both Parquet and PostgreSQL's `analytics` schema. Orchestration runs in two modes: `--full-refresh` runs extract→silver→gold→warehouse→quality; `--incremental` skips extract and reuses cached Bronze for faster re-runs when APIs are unavailable. The warehouse feeds a Power BI dashboard in Import mode for daily-refreshed cross-domain analytics.

---

### Q2: How did you handle API failures?

API calls to Open-Meteo and Frankfurter are wrapped with tenacity decorators implementing exponential backoff with jitter, allowing transient failures to retry before eventually logging and degrading gracefully. The Bronze layer persists API responses as Parquet snapshots on disk, so `--incremental` simply skips extraction and reuses yesterday's cached weather and FX data — the pipeline does not fail if an API is temporarily down. Extraction errors are logged via loguru with full context (timestamp, source, attempt count); the Silver-layer pandera validation then verifies that stale cached data still conforms to schema. If an API is permanently unavailable, the quarantine layer captures affected rows with explicit `quarantine_reason` labels, and post-load quality checks in `analytics.data_quality_log` flag elevated rejection rates so the problem surfaces before the dashboard consumes bad data.

---

### Q3: Why the medallion architecture?

The medallion pattern enforces separation of concerns and debuggability. Bronze preserves raw, unmodified source data so any Gold fact can be traced back to its origin and transformation decisions audited. Silver is a strict validation checkpoint where pandera catches schema violations, type mismatches, and business rule breaches before they reach analytics — and the `quarantine/` directory becomes a searchable audit trail of exactly what failed and why. This design makes Silver rerunnable without re-hitting APIs (Bronze is already cached), cuts debugging time by localising where a data anomaly originated, and enables re-validating old Bronze runs against updated Silver schemas. These are the same properties that make this pattern the industry standard for modern lakehouses (Delta Lake, Iceberg, Hudi) — the scale differs, the pattern is identical.

---

### Q4: How do you handle data quality?

Quality gates are layered at two points. At the Silver stage, pandera schemas run with `lazy=True`, collecting all violations before raising so a single row can be tagged with multiple failure descriptions (e.g., missing `customer_id` AND invalid `order_date`); rejected rows are written to `quarantine/` with `quarantine_reason` and `quarantined_at` for root-cause analysis. The `log_transform_summary()` function also emits a WARNING when any stage shows a drop rate ≥ 10%, alerting operators to unexpected rejection spikes before the pipeline continues. After the warehouse load, automated SQL checks write results to `analytics.data_quality_log` recording source, table, row counts, and pass/fail status. The combination of pre-load schema validation and post-load metrics ensures every rejected row is catalogued and every major anomaly is surfaced before dashboards consume the data.

---

### Q5: What was the hardest debugging problem?

The hardest issue was a subtle bug in `transform_fx()` during the date gap-fill step. The function reindexes the daily FX DataFrame to a full calendar range to ensure no weekend or holiday gaps; after reindex, it calls `reset_index()` to restore "date" as a column. The bug: if the DatetimeIndex wasn't explicitly named `"date"` before reindex, pandas' `reset_index()` produced a column named `"index"` instead, causing a `KeyError` in downstream merge logic minutes later in the Gold stage. The failure was silent during the FX transform itself — no exception, just a mislabelled column — making it difficult to trace. The fix was one line: `df.index.name = "date"` before `df.reindex(full_range)`. The lesson: in pandas, index names are load-bearing — always set them explicitly before operations that derive column names from index metadata, and test the column names of function outputs, not just their values.

---

### Q6: How would you scale this to larger data volumes?

For scale, the first change would be partitioning Bronze and Silver layers by year-month, enabling parallel per-partition transforms that exploit multiple CPU cores and avoid loading full history into memory. The Silver pandas code would be a candidate for DuckDB (columnar, pushdown-friendly, no cluster required) or Spark for distributed processing. Orchestration would move from Python `--flags` to Airflow or Prefect, enabling task-level retry, SLA monitoring, and dependency graphs. For the warehouse, `fact_sales` (currently ~112k rows) would be pushed to BigQuery or Snowflake for sub-second analytics queries, replacing Power BI Import mode with DirectQuery or a Semantic Layer. Olist extraction would upgrade from full CSV reload to incremental change-data-capture if the source database were accessible. Testing would expand to integration tests against a staging warehouse with synthetic data generators to stress-test schema changes before production. This path is straightforward — the medallion pattern and star schema are designed to scale; you'd change the execution engine, not the architecture.

---

## Additional Interview Angles

### "How long did this take and how did you manage it?"

This was a 6-week solo build broken into 14 deliberate stages. I enforced a gate after each major layer — Bronze, Silver, Gold — validating assumptions before moving forward rather than trying to ship everything at once. The tightest constraint was the Power BI dashboard: I documented the semantic model design first (Stage 8), then built DAX measures (Stage 9) before touching certain data pipeline decisions, which forced prioritisation on what analyses actually mattered. I deferred testing and CI until Stage 13 — a deliberate trade-off: get the core pipeline working and learn what needs testing, then harden it. Starting with tests would have front-loaded complexity before I understood the full contract of each function.

### "What would you do differently?"

First, I'd extract only new date ranges from the APIs instead of full re-pulls. Both Open-Meteo and Frankfurter support date-windowed requests; adding a cache-check pattern would cut API calls and runtime significantly. Second, I'd use dbt for Silver and Gold transforms instead of pandas. The current pandas code is functional, but dbt provides SQL lineage, auto-generated documentation, and built-in testing without additional tooling — and it makes transforms portable to any SQL warehouse. Third, I'd add geolocation enrichment from the Olist geolocation table earlier in the pipeline; zip-code-to-coordinates data is already in Bronze but currently unused in the Gold schema.

### "What's the most impressive part?"

The cross-source join complexity. Weather required fuzzy city-name matching — Unicode normalisation to strip accents, then a rapidfuzz similarity threshold — followed by a date join across 112k order-items. FX rates introduced a cross-rate derivation: Frankfurter publishes only EUR-base pairs, so BRL/USD is computed as (EUR/BRL) ÷ (EUR/USD), introducing two sources of rate error that are explicitly documented in the schema. Layered on top of that is the quarantine pattern with pandera: every invalid row is isolated at the Silver boundary with a rejection reason rather than silently dropped or passed through. The full-stack scope — Python pipelines, PostgreSQL star schema, Power BI with 27 DAX measures, and GitHub Actions CI — distinguishes this from portfolio projects that show one layer in isolation.

### "How did you decide what to build vs. skip?"

This is a portfolio project, not production software. I optimised for demonstrating breadth of enterprise data engineering skills to a technical hiring manager, not architectural perfection. I skipped SCD Type 2 because the dimension tables are small and stable — adding temporal tracking would complicate the schema with no analytical value on a static dataset. I skipped CDC and Airflow for the same reason: the source data is a fixed Kaggle export, so incremental loading adds no real value here, though I designed the orchestration modes (`--full-refresh` / `--incremental`) to demonstrate the pattern. I also skipped dbt on the initial build — shipping a working pandas pipeline first, then documenting where dbt would fit, shows that I can prioritise iteration over premature architecture.

---

## Known Limitations (be ready to own these)

| Limitation | Honest explanation |
|------------|-------------------|
| Weather coverage limited to top 20 cities (~61% miss) | Open-Meteo extractor pulls only the top 20 cities by order volume; broaden via `WEATHER_CITY_COUNT` env var |
| FX is mid-market USD/BRL only | Frankfurter direct USD→BRL pair, weekend/holiday-gap forward-filled; not transaction-grade |
| Olist is a full reload, not incremental | Kaggle has no delta API; `--incremental` skips only the extract stage |
| Power BI is Import mode, not real-time | ~60 MB model, daily refresh sufficient for this dataset |
| review_score not in Gold schema | review_id has duplicates; decision was to quarantine rather than deduplicate ambiguously |
