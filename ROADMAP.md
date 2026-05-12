# Roadmap

Remaining work to fully deliver the project. Updated **2026-05-12** at HEAD `5fd657f`.

This file is session-sequenced — it describes *what to do next*, in order, with
realistic effort estimates. For the conceptual feature list (what's missing,
why it matters), see the **Future Improvements** section in `README.md`.

---

## Block 1 — Close `fact_payments` (Phase 4) — ~30 min

The data is already in Gold Parquet at `data/gold/facts/fact_payments.parquet`;
this block lands it in the warehouse and finishes the documentation.

| Task | File |
|---|---|
| Add warehouse loader entry | `src/load/load_to_warehouse.py:59-63` (`_FACT_TABLES`) — `("fact_payments", "fact_payments", ["order_id", "payment_sequential"])` |
| Add 5 quality checks | `src/quality/checks.py` — row_count threshold, no-nulls on PK + `payment_value`, uniqueness on `(order_id, payment_sequential)`, range `payment_value > 0`, RI on `date_key → dim_date`. Also add `fact_payments` to `_ANALYTICS_TABLES` allowlist. |
| Add unit tests (~4) | `tests/test_schemas.py`, `tests/test_transform_functions.py` |
| README sync | Move `fact_payments` from Roadmap → Delivered; bump test counts 95→99 / 98→102; drop the "loader pending" caveats across `README.md`, `main.py`, `load_to_warehouse.py`, SQL DDL headers, `stage_warehouse` docstring |
| Verify | `make init` + `make full-refresh` + smoke `SELECT * FROM analytics.fact_payments LIMIT 5` |

**Blocked by:** nothing. **Highest-leverage next session.**

---

## Block 2 — `dim_geolocation` — ~3 hr

Adds lat/lon enrichment to `dim_customer` and `dim_store`. Biggest visual
unlock for heatmap and distance-based analytics.

| Task | File |
|---|---|
| Dedup policy: **median** per `zip_prefix` (decided — see Decisions section) | — |
| `source_system.geolocation` DDL (PK: `zip_prefix` post-dedup) | `sql/ddl/03_source_system.sql` |
| `load_geolocation()` function | `src/setup/load_source_db.py` |
| Add `"geolocation"` to `BRONZE_DB_TABLES` | `src/extract/config.py` |
| Remove `"geolocation"` from `_RAW_OLIST_SNAPSHOTS` | `src/extract/extract_olist_csvs.py` |
| `SilverGeolocationSchema` | `src/transform/schemas.py` |
| Silver transform (with dedup) | new `src/transform/transform_geolocation.py` |
| Enrich `dim_customer` + `dim_store` | `src/transform/build_dimensions.py` — add `latitude` / `longitude` via zip_prefix join |
| Add lat/lon to dim DDL | `sql/ddl/04_gold_schema.sql` (with `ALTER TABLE … ADD COLUMN IF NOT EXISTS` for existing DBs) |
| Tests | unit tests for schema + transform |
| README | drop `dim_geolocation` from Roadmap; mention lat/lon in dim_customer/dim_store; remove the "no heatmap" caveat |

**Blocked by:** nothing. After this, `BRONZE_DB_TABLES` is 6 → 7; unmodelled
CSVs drop 3 → 2.

---

## Block 3 — `fact_reviews` — ~4 hr

| Task | File |
|---|---|
| Dedup policy: **none — keep all rows; PK is a SERIAL `review_id_int`; uniqueness enforced on `(review_id, order_id)`** (decided — see Decisions section) | — |
| `source_system.reviews` DDL (SERIAL PK + UNIQUE on `(review_id, order_id)`) | `sql/ddl/03_source_system.sql` |
| `load_reviews()` function (with dedup) | `src/setup/load_source_db.py` |
| Add `"reviews"` to `BRONZE_DB_TABLES` | `src/extract/config.py` |
| Remove `"reviews"` from `_RAW_OLIST_SNAPSHOTS` | `src/extract/extract_olist_csvs.py` |
| `SilverReviewsSchema` | `src/transform/schemas.py` |
| Silver transform | new `src/transform/transform_reviews.py` |
| Gold fact builder | `src/transform/build_facts.py` (`build_fact_reviews`) |
| `analytics.fact_reviews` DDL | `sql/ddl/04_gold_schema.sql` |
| Warehouse loader entry | `src/load/load_to_warehouse.py` |
| 5 quality checks | `src/quality/checks.py` |
| Unit tests | `tests/test_schemas.py`, `tests/test_transform_functions.py` |
| README | move `fact_reviews` from Roadmap to Delivered; remove "review data is Bronze-only" Known Limitation; restore BQ5 satisfaction question; bump test counts |

**Blocked by:** dedup policy decision. After this, unmodelled CSVs drop to 1
(`category_translation`) — fold that in too (~30 min) and Bronze is fully
modelled.

---

## Block 4 — Author the `.pbix` — ~1–2 days

Requires a Windows VM (Power BI Desktop is Windows-only).

| Task | Where |
|---|---|
| Set up Parallels VM with Windows 11 | macOS host |
| Install Power BI Desktop + Npgsql driver | Inside VM |
| Confirm `host.docker.internal:5433` reachable | Per `docs/powerbi_vm_workflow.md` |
| Set `powerbi_reader` password | `ALTER ROLE powerbi_reader WITH PASSWORD '…'` via psql — replaces the literal placeholder in `sql/ddl/06_powerbi_readiness.sql:65` |
| Build semantic model | Per `docs/POWER_BI_SEMANTIC_MODEL_DESIGN.md` — Import mode, role-playing currency, calendar marked |
| Implement 27 DAX measures | Per `docs/stage9_dax_measures.md` |
| Build 4 pages | Per `docs/stage10_dashboard_pages.md` |
| Export PNG screenshots | To `docs/screenshots/01_executive_overview.png` etc. |
| Save `.pbix` | To `pbix/multi_source_etl.pbix` (git-ignored) |

**Blocked by:** Windows VM access.

---

## Block 5 — Documentation final pass — ~1 hr

Runs **after Block 4**, so doc claims match the actual delivered state.

| Task | Where |
|---|---|
| Drop "(designed)" qualifier from Skills row | `README.md:464` |
| Remove "partially stale" banners from PBI docs | `docs/stage8/9/10_*.md`, `docs/POWER_BI_SEMANTIC_MODEL_DESIGN.md` |
| Replace inline EUR-base FX cross-rate DAX with direct USD/BRL | Same 4 docs |
| Update PBI placeholder row counts (500K/20K) → real (~112,650) | `docs/POWER_BI_SEMANTIC_MODEL_DESIGN.md:64-65, 498, 747-748` |
| Remove review-score aspirational sections | Same doc (after Block 3 lands) |
| Update README headline to fully-delivered state | E.g. "5 dims + 4 facts in PostgreSQL; 4-page Power BI dashboard delivered" |

---

## Block 6 — Final codex audit + commit — ~30 min

One last codex sweep to catch ripple drift, fix anything outstanding, push the
final commit. Session pattern has been that every commit creates small ripple
drift — expect ~3–5 items to fix in this pass.

---

## Dependency graph

```
Block 1 (fact_payments Phase 4) ─┐
                                 ├─→ Block 5 (doc final pass) ─→ Block 6 (final audit)
Block 2 (dim_geolocation)     ───┤
                                 │
Block 3 (fact_reviews)        ───┤
                                 │
Block 4 (.pbix authoring) ────────┘
```

Blocks 1–3 are independent and can be done in any order. Block 4 can run in
parallel inside the VM if you have it. Block 5 depends on Block 4 (so doc
claims match reality). Block 6 closes everything.

---

## Realistic session sequencing

| Session | Block | Time |
|---|---|---|
| Next | Block 1 — close `fact_payments` | 30 min |
| Following | Block 2 — `dim_geolocation` (heatmap capability) | 3 hr |
| Following | Block 3 — `fact_reviews` (satisfaction analytics) | 4 hr |
| Following | Block 4 — `.pbix` authoring (Windows VM) | 1–2 days |
| Final | Block 5 + 6 — doc cleanup + codex audit | 1.5 hr |

**Total ~3 working days** to a fully-delivered portfolio project. Without
Block 4 (the `.pbix`), it's ~7.5 hours of code work — but the `.pbix` is the
headline portfolio artifact, so skipping it leaves the biggest credibility gap.

---

## What to do next

**If you have 30 minutes:** Block 1. Closes a started feature, biggest README
cleanup, fast wins.

**If you have 3 hours:** Block 1 + Block 2. Heatmap-ready dim_customer +
dim_store enables a meaningfully different dashboard.

**If you have a full day:** Block 1 + 2 + 3. Bronze becomes fully modelled
(if you also fold in `category_translation`); README's "Roadmap" section
becomes nearly empty.

**If you have a Windows VM and a weekend:** Block 4. The headline payoff.

---

## Decisions & accepted caveats

Captured here so a fresh reader (human or AI) doesn't re-litigate them.

### Design decisions (load-bearing)
- **`fact_payments` grain** = one row per `(order_id, payment_sequential)`,
  with `order_code` carried as a degenerate dimension for source
  traceability. Preserves split-payment detail (~3% of orders pay via
  multiple instruments, max 27 payments on one order). Aggregations to
  per-order grain happen downstream in DAX/SQL. *(Decided 2026-05-11,
  commit `b72742c`; PK at `sql/ddl/04_gold_schema.sql:402-413`.)*
- **`dim_customer.normalized_city`** = NFD-stripped lowercase, computed at
  Gold-build time via `src/utils/validators.normalize_city_name`. Chosen
  over a Postgres `unaccent` extension because (a) avoids extension
  dependency, (b) Open-Meteo's `DEFAULT_CITIES` list at
  `src/extract/extract_weather.py:58-79` is already accent-free at extract
  time (e.g. `sao_paulo`, not `São Paulo`), so applying NFD-strip + lower
  to `dim_customer.city` produces an exact match against
  `fact_weather_daily.city`. Note: Silver weather transform lowercases /
  trims but does NOT strip accents — accent-freeness comes from the
  upstream city list. *(Decided round-2 D1.)*
- **`v_sales_with_weather`** uses `LEFT JOIN dim_customer` (not INNER) since
  `fact_sales.customer_key` is nullable per `sql/ddl/04_gold_schema.sql:274-281`.
  INNER JOIN silently drops null-FK sales rows. *(Bug found by codex
  round-3 at `d81b817`.)*
- **`v_sales_usd`** filters `base_currency='USD' AND quote_currency='BRL'`
  inside the `ON` clause, not `WHERE`. Predicates in `WHERE` would collapse
  the `LEFT JOIN` to inner and drop sales rows on dates outside Frankfurter's
  trading-day coverage. *(Codex round-2 catch.)*
- **`fact_sales` line-item grain stays single-currency (BRL)** —
  `v_sales_enriched` joins to 4 of 5 dimensions; `dim_currency` is omitted
  because all Olist data is BRL today. Use `v_sales_usd` for USD reporting.
- **Bronze schema mismatch isolation:** `BRONZE_DB_TABLES` (in
  `src/extract/config.py`) and `_RAW_OLIST_SNAPSHOTS` (in
  `src/extract/extract_olist_csvs.py`) are mutually exclusive. When a table
  moves from "raw snapshot" to "modelled in `source_system`", remove from the
  latter as you add to the former, or two writers will land in
  `data/bronze/db/<table>/` and Silver picks the wrong file. *(Bug hit at
  `b72742c` first attempt.)*
- **Block 2 `dim_geolocation` dedup = median per `zip_prefix`.** The Olist
  geolocation CSV has ~1,000,163 rows for ~19,015 unique zip prefixes, with
  some prefixes (e.g. `57319`) carrying outlier coordinates that span
  ~3,251 km between min/max latitude. Mean is pulled by these outliers;
  median is robust. Implementation: in the Silver transform, do
  `df.groupby('zip_code_prefix').agg({'latitude':'median','longitude':'median'})`,
  then load to `source_system.geolocation` with `zip_prefix` as PK. Refer to
  the documented many-to-one issue in `docs/source_schema.md:390`.
  *(Decided 2026-05-12 via codex consultation; agreed with codex's
  recommendation.)*
- **Block 3 `fact_reviews` dedup = none; preserve all rows.** The 789
  duplicate `review_id` values in the source (1,603 duplicate rows total)
  are NOT competing versions of the same review — they are the SAME review
  attached to MULTIPLE orders. A sampled duplicate (`c444278…`) appears on
  3 orders with identical `review_score=5` and identical
  `review_answer_timestamp`, only `order_id` differs. Dropping duplicates
  by any heuristic (last by timestamp, first, highest score) would
  arbitrarily lose valid order-review associations. Decision: use a
  SERIAL surrogate PK in `source_system.reviews`, enforce `UNIQUE (review_id,
  order_id)` instead of `UNIQUE (review_id)`. The "review_id is unreliable
  as a primary key" caveat in `docs/source_schema.md:377-385` is consistent
  with this choice. *(Decided 2026-05-12 via codex consultation; agreed
  with codex's recommendation.)*

### Accepted caveats (intentionally left for later)
These are flagged behind banners; do **not** re-fix unless explicitly asked.

- **Inline FX cross-rate text** in `docs/stage8_powerbi.md`,
  `docs/stage9_dax_measures.md`, `docs/stage10_dashboard_pages.md`,
  `docs/POWER_BI_SEMANTIC_MODEL_DESIGN.md`. The pipeline now fetches direct
  USD/BRL (`extract_fx.py`); the docs were written when only EUR-base pairs
  were available. Top-of-file banners point readers to `v_sales_usd`.
  Rewriting the inline DAX is Block 5 work.
- **PBI placeholder row-count estimates** (500K / 20K) in
  `docs/POWER_BI_SEMANTIC_MODEL_DESIGN.md:64-65, 498, 747-748` vs reality
  (~112,650 sales rows). Same situation — banner'd, deferred to Block 5.
- **Review-score DAX measures** in `POWER_BI_SEMANTIC_MODEL_DESIGN.md`. Banner
  marks them aspirational; will become real once Block 3 (`fact_reviews`)
  lands.
- **Weather coverage ~61% miss.** By design — Open-Meteo extractor pulls only
  the top 20 cities by order volume. Raising `WEATHER_CITY_COUNT` in `.env`
  and re-extracting closes the gap; not a bug.

### Codex audit cadence
This project has gone through 6 codex audit rounds in one session. Every
significant code commit produces ~5–20 ripple drift sites elsewhere (other
docs, docstrings, SQL header comments). Default cadence: run codex after
each major feature commit, then triage findings before applying. See
`feedback_review_rigor.md` (memory) for the "verify findings against source
before applying" rule.

