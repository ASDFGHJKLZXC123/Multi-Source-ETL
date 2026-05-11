# Stage 8 — Power BI Connection & Data Model

> **⚠ FX section is partially stale.** This document was written when
> Frankfurter only published EUR-base pairs and BRL/USD had to be derived
> as a cross-rate. The pipeline now extracts the **direct USD/BRL pair**
> via `src/extract/extract_fx.py` and stores one row per calendar day in
> `analytics.fact_fx_rates` (`base_currency='USD'`, `quote_currency='BRL'`).
> Use the pre-joined view `analytics.v_sales_usd` (defined in
> `sql/ddl/06_powerbi_readiness.sql`) instead of computing
> `EUR/BRL ÷ EUR/USD` cross-rates in DAX. Sections 6.x below that reference
> EUR-base measures should be treated as design history.

> **Pipeline stage:** 8 of 8  
> **Schema target:** `analytics` (PostgreSQL Gold layer)  
> **Companion SQL:** `sql/ddl/06_powerbi_readiness.sql`

---

## 1. Connection Plan

### 1.1 Prerequisites

| Requirement | Detail |
|---|---|
| Npgsql driver | Install **Npgsql 6.0.x** standalone MSI from the Npgsql GitHub releases page before opening Power BI Desktop. Power BI's bundled Npgsql (4.0.x) mishandles `TIMESTAMPTZ` → `DateTimeOffset` mapping. Restart Power BI Desktop after installation. |
| PostgreSQL user | `powerbi_reader` — read-only role created by `06_powerbi_readiness.sql`. Never use a superuser or ETL pipeline user. |
| Network access | Power BI Desktop machine must reach the PostgreSQL host on port 5432 (or via SSH tunnel / VPN as required). |

### 1.2 Power BI Desktop Connection Steps

1. **Home → Get Data → Database → PostgreSQL Database → Connect**
2. Fill the connection dialog:

   | Field | Value |
   |---|---|
   | Server | `<pg-host>:5432` |
   | Database | `<database-name>` |
   | Data Connectivity mode | **Import** (see Section 2) |
   | Advanced options | `options=-csearch_path=analytics` |

3. Authentication: **Database** → enter `powerbi_reader` credentials.
4. In the Navigator, expand the `analytics` node and select all 8 tables:
   `dim_date`, `dim_customer`, `dim_product`, `dim_store`, `dim_currency`,
   `fact_sales`, `fact_weather_daily`, `fact_fx_rates`.
5. Click **Transform Data** (not Load) to enter Power Query for renaming and the `dim_date` integer conversion before loading.

> **Why `search_path=analytics` in advanced options?**  
> Without it, Power BI's auto-generated SQL may omit the schema prefix.
> The `powerbi_reader` role also has `search_path = analytics, pg_catalog`
> set at the role level (`06_powerbi_readiness.sql`) as a belt-and-suspenders
> measure, but the connection-string option ensures folding queries resolve
> correctly regardless of session state.

---

## 2. Import vs DirectQuery Decision

**Decision: Import mode for all tables.**

| Factor | Detail |
|---|---|
| Data volume | ~112,650 fact_sales rows, ~1,000+ weather rows, ~500+ FX rows — well under Import mode practical limits (~50 M rows). Total model ≈ 60 MB compressed. |
| Refresh latency | ETL runs daily (full-refresh or incremental). Power BI scheduled refresh once per day is sufficient; sub-minute latency is not required. |
| DAX time intelligence | `TOTALYTD`, `DATEADD`, and other calendar functions require an in-memory date table. They do not work in DirectQuery. |
| Query performance | Import mode serves all DAX from a compressed in-memory columnar store; no round-trip to PostgreSQL per visual. Dashboard interaction is instant. |
| Incremental refresh | `_loaded_at TIMESTAMPTZ` columns on all fact tables support Power BI incremental refresh for future scale. See Section 6.5. |

DirectQuery is **not recommended** for this project. The dataset is small, the ETL refresh cycle is daily, and DAX time intelligence is a hard requirement.

---

## 3. Semantic Model Design

### 3.1 Table Display Names

Rename in Power Query (Transform Data → right-click query → Rename) before loading.

| Technical Name | Display Name | Rationale |
|---|---|---|
| `fact_sales` | Sales Transactions | Communicates grain; avoids DW jargon. |
| `fact_weather_daily` | Daily Weather Conditions | "Daily" states the grain explicitly. |
| `fact_fx_rates` | Exchange Rates | Plain English; no "fact" prefix. |
| `dim_date` | Calendar | Standard Power BI convention for the date table. |
| `dim_customer` | Customers | No technical prefix. |
| `dim_product` | Products | Consistent naming pattern. |
| `dim_store` | Sellers | Business term for marketplace participants (not "stores"). |
| `dim_currency` | Currencies | Plural noun, no prefix. |

### 3.2 Relationship Design

Define all relationships manually in Power BI Model view — do not rely on auto-detection. Verify each one against the list below.

| # | From table / column | To table / column | Cardinality | Cross-filter | Active |
|---|---|---|---|---|---|
| R1 | Sales Transactions[date_key] | Calendar[date_key] | Many → One | Single (→ Calendar) | Yes |
| R2 | Sales Transactions[customer_key] | Customers[customer_key] | Many → One | Single (→ Customers) | Yes |
| R3 | Sales Transactions[product_key] | Products[product_key] | Many → One | Single (→ Products) | Yes |
| R4 | Sales Transactions[store_key] | Sellers[store_key] | Many → One | Single (→ Sellers) | Yes |
| R5 | Sales Transactions[currency_key] | Currencies[currency_key] | Many → One | Single (→ Currencies) | Yes |
| R6 | Daily Weather Conditions[date_key] | Calendar[date_key] | Many → One | Single (→ Calendar) | Yes |
| R7 | Exchange Rates[date_key] | Calendar[date_key] | Many → One | Single (→ Calendar) | Yes |
| R8 | Exchange Rates[base_currency_key] | Currencies[currency_key] | Many → One | Single (→ Currencies) | Yes |
| R9 | Exchange Rates[quote_currency_key] | Currencies_Quote[currency_key] | Many → One | Single (→ Currencies_Quote) | Yes |

**Role-playing currency dimension (R8 / R9):**  
Power BI does not allow two active relationships from one table to the same target table. To handle `base_currency_key` and `quote_currency_key` both referencing `dim_currency`:

1. In Power Query, duplicate the `Currencies` query and rename the copy to `Currencies (Quote)`.
2. R8 (`base_currency_key`) uses the original `Currencies` table — keep active.
3. R9 (`quote_currency_key`) uses `Currencies (Quote)` — keep active.
4. DAX measures that need quote-side currency labels use `RELATED('Currencies (Quote)'[currency_code])`.

**Nullable FKs on Sales Transactions:**  
`customer_key`, `product_key`, and `store_key` are nullable (Silver orphan rows). Power BI handles NULL FK values by routing those rows to the blank member of the dimension — this is correct behaviour. Do not filter out NULL-FK rows in Power Query; they represent real transactions with unresolvable dimension keys.

**fact_weather_daily degenerate dimensions:**  
`city` and `state` in `fact_weather_daily` are degenerate dimension columns — they live in the fact table itself. There is no separate city/state dimension table. Report slicers for weather city/state should reference these columns directly from the `Daily Weather Conditions` table.

### 3.3 Mark dim_date as the Date Table

This step is required for all DAX time intelligence functions to work correctly.

1. Select the `Calendar` table in Model view.
2. **Table tools ribbon → Mark as date table → Mark as date table**.
3. In the dialog, select the `date` column (type: Date). This is a Power Query-derived column — see Section 3.4.
4. Power BI validates that the column has no gaps and no duplicates across the date range. The check must pass before the dialog closes.

> Do **not** mark `date_key` (the YYYYMMDD integer) as the date table column.
> The `date` column (a true Date type derived in Power Query) must be used.

### 3.4 dim_date Integer Key → Date Column

`date_key` is stored as INT in YYYYMMDD format (e.g., `20171001`). Power BI imports it as `Whole Number` and does not recognise it as a date.

In Power Query, on the `Calendar` query, add a custom column **before loading**:

```m
Date = #date(
    Number.IntegerDivide([date_key], 10000),
    Number.IntegerDivide(Number.Mod([date_key], 10000), 100),
    Number.Mod([date_key], 100)
)
```

Set its data type to **Date**. This column is used to mark the date table and for DAX time intelligence. The integer `date_key` column remains as the relationship key — do not remove it.

### 3.5 Column Hiding

Hide all of the following from the report view. They remain available to relationship and DAX logic.

**Surrogate keys (join columns only):**
`date_key`, `customer_key`, `product_key`, `store_key`, `currency_key`,
`base_currency_key`, `quote_currency_key`

**Source system IDs (ETL internal):**
`customer_id`, `product_id`, `store_id`, `order_item_id` (hide unless grain-level drilling is required)

**Audit columns:**
`_loaded_at` on every table

> **Keep visible:** `base_currency` and `quote_currency` (CHAR(3) text columns on `Exchange Rates`) must **not** be hidden. The FX DAX measure stubs in Section 4.1 filter on these columns directly (`'Exchange Rates'[base_currency] = "EUR"`). Hiding them would break those measures.

### 3.6 Key Column Renames

Apply these renames in Power Query to improve report readability.

| Table | Current name | Display name |
|---|---|---|
| Sales Transactions | `unit_price` | Item Price (BRL) |
| Sales Transactions | `freight_value` | Freight Cost (BRL) |
| Sales Transactions | `order_code` | Order Reference |
| Customers | `zip_code_prefix` | Postcode Prefix |
| Customers | `state` | Customer State |
| Customers | `city` | Customer City |
| Sellers | `state` | Seller State |
| Sellers | `zip_code_prefix` | Seller Postcode Prefix |
| Products | `weight_g` | Shipping Weight (grams) |
| Products | `category_name_en` | Category (English) |
| Daily Weather Conditions | `temp_max` | Max Temperature (°C) |
| Daily Weather Conditions | `temp_min` | Min Temperature (°C) |
| Daily Weather Conditions | `precipitation` | Precipitation (mm) |
| Daily Weather Conditions | `windspeed` | Wind Speed (km/h) |
| Daily Weather Conditions | `weathercode` | Weather Condition Code |

---

## 4. _Measures Table

Create a disconnected placeholder table to keep all DAX measures organised in one place, separate from the data tables.

**Create the table:**

1. Home ribbon → Enter Data.
2. Create a one-column, one-row table with column name `[Placeholder]` and value `" "` (a single space — so the column is not blank).
3. Name the table `_Measures`.
4. Delete the `Placeholder` column after the table is loaded (or hide it).
5. The table will appear at the top of the Fields pane (leading underscore sorts it first).

**Do not create relationships** from `_Measures` to any other table.

### 4.1 Initial DAX Measure Stubs

Add the following measures to the `_Measures` table. These are the baseline set; expand as reporting requirements grow.

```dax
-- Revenue & Volume
Total Revenue =
    SUMX(
        'Sales Transactions',
        'Sales Transactions'[Item Price (BRL)] * 'Sales Transactions'[quantity]
    )

Total Orders =
    DISTINCTCOUNT('Sales Transactions'[order_code])

Total Items Sold =
    COUNTROWS('Sales Transactions')

Avg Order Value =
    DIVIDE([Total Revenue], [Total Orders])

-- Freight
Total Freight =
    SUM('Sales Transactions'[Freight Cost (BRL)])

Avg Freight per Order =
    DIVIDE([Total Freight], [Total Orders])

Freight % of Revenue =
    DIVIDE([Total Freight], [Total Revenue], 0)

-- Time intelligence (requires Calendar marked as date table)
Revenue MoM % =
    VAR _cur = [Total Revenue]
    VAR _prev = CALCULATE([Total Revenue], DATEADD(Calendar[date], -1, MONTH))
    RETURN DIVIDE(_cur - _prev, _prev)

Revenue YTD =
    TOTALYTD([Total Revenue], Calendar[date])

-- Exchange rates
Latest EUR/BRL Rate =
    CALCULATE(
        LASTNONBLANK('Exchange Rates'[rate], 1),
        'Exchange Rates'[base_currency] = "EUR",
        'Exchange Rates'[quote_currency] = "BRL"
    )

Latest EUR/USD Rate =
    CALCULATE(
        LASTNONBLANK('Exchange Rates'[rate], 1),
        'Exchange Rates'[base_currency] = "EUR",
        'Exchange Rates'[quote_currency] = "USD"
    )

-- DEPRECATED: cross-rate derivation kept here as design history.
-- The current pipeline fetches USD/BRL directly via src/extract/extract_fx.py.
-- Use analytics.v_sales_usd or:
--   USD per BRL = LASTNONBLANK('Exchange Rates'[rate], 1)
--                 with base_currency='USD' AND quote_currency='BRL'
-- BRL/USD cross-rate (legacy when only EUR-base was available):
BRL per USD =
    DIVIDE([Latest EUR/BRL Rate], [Latest EUR/USD Rate])

-- Weather
Avg Max Temperature =
    AVERAGE('Daily Weather Conditions'[Max Temperature (°C)])

Temperature Range =
    AVERAGE('Daily Weather Conditions'[Max Temperature (°C)])
    - AVERAGE('Daily Weather Conditions'[Min Temperature (°C)])
```

---

## 5. Model View Layout

Arrange tables in Model view as a clean star schema. Suggested positions:

```
                        [ Calendar ]
                             |
    [ Customers ]   [ Sales Transactions ]   [ Products ]
                             |
                         [ Sellers ]

    [ Daily Weather Conditions ]    [ Exchange Rates ]
              |                            |      |
          [ Calendar ]           [ Currencies ]  [ Currencies (Quote) ]
```

**Layout rules:**
- Place `Calendar` at the top-centre — it connects to all three facts.
- Place `Sales Transactions` in the centre row.
- Dimension tables radiate outward from `Sales Transactions`.
- Place `Daily Weather Conditions` and `Exchange Rates` in the lower half, with their own dimension connections.
- Place `_Measures` in the upper-left corner, clearly separated from the schema.

---

## 6. Advanced Considerations

### 6.1 BRL/USD Cross-Rate

The Frankfurter API (ECB data) uses EUR as its base currency. There is no direct BRL/USD pair in `fact_fx_rates`. Compute it as:

```
BRL/USD = EUR/BRL rate ÷ EUR/USD rate
```

Use the `BRL per USD` measure stub in Section 4.1. For a permanent fix, request the ETL team add a derived `BRL/USD` row in the Silver FX transform.

### 6.2 Weather–Sales Join

`fact_weather_daily` shares no FK with `fact_sales`. The join path requires:
`fact_sales` → `dim_customer` (customer city + state) → `fact_weather_daily` (city + state + date_key).

Power BI does not support composite relationship keys. Options (in order of preference):

1. **PostgreSQL view** — `analytics.v_sales_enriched` (created by `06_powerbi_readiness.sql`) pre-joins `fact_sales` to four of its five FK dimensions (dim_date, dim_customer, dim_product, dim_store — dim_currency omitted, since current data is BRL-only), returning `customer_city`, `customer_state` alongside the fact measures. Import this view alongside `Daily Weather Conditions` and join on a composite WeatherBridgeKey (see option 2).
2. **Power Query composite key** — add a calculated column `WeatherBridgeKey = [Customer City] & "|" & [Customer State] & "|" & Text.From([date_key])` in both `Customers` and `Daily Weather Conditions`, then relate on that key.
3. **DAX TREATAS** — advanced; use only if option 1 and 2 are not viable.

### 6.3 Cancelled Orders

Cancelled orders are excluded from `fact_sales` by the Silver transform quarantine step. All metrics reflect non-cancelled transactions by default. This is the correct behaviour and should be disclosed in report titles (e.g., "Excludes cancelled orders").

### 6.4 Row-Level Security

Two RLS scenarios are available when publishing to Power BI Service:

**Scenario A — Customer State RLS** (restrict data to customer's region):
Apply a filter on `Customers[Customer State]` using a `UserStateBridge` lookup table (UserEmail → State). Propagates to `Sales Transactions` through the R2 relationship.

**Scenario B — Seller RLS** (restrict sellers to their own data):
Apply a filter on `Sellers[store_code]` or `Sellers[Seller State]` using the same bridge pattern.

Do not apply RLS to `Daily Weather Conditions`, `Exchange Rates`, or `Calendar` — these are reference tables visible to all users.

### 6.5 Incremental Refresh

All fact tables carry `_loaded_at TIMESTAMPTZ`. To enable Power BI incremental refresh:

1. Create parameters `RangeStart` and `RangeEnd` (type: DateTime) in Power Query.
2. Add a filter step on `_loaded_at` in each fact query using these parameters.
3. Ensure this step folds to SQL (right-click → View Native Query must be available).
4. `06_powerbi_readiness.sql` creates `_loaded_at` indexes on all three fact tables to support efficient range scans.

Prerequisite: `ALTER ROLE powerbi_reader SET timezone = 'UTC'` (included in `06_powerbi_readiness.sql`) ensures consistent UTC presentation of `TIMESTAMPTZ` values.

---

## 7. Business Metrics Reference

Key measures for report pages. Full catalogue in `docs/POWER_BI_SEMANTIC_MODEL_DESIGN.md`.

| Business Name | DAX stub | Owner |
|---|---|---|
| Total Revenue (BRL) | `SUMX(fact_sales, price * qty)` | Sales, CFO |
| Total Orders | `DISTINCTCOUNT(order_code)` | Sales, Ops |
| Total Items Sold | `COUNTROWS(fact_sales)` | Ops, Catalogue |
| Avg Order Value | `DIVIDE(Revenue, Orders)` | Sales, Marketing |
| Freight % of Revenue | `DIVIDE(freight, revenue, 0)` | CFO, Ops |
| Revenue MoM % | `DATEADD(-1 MONTH)` pattern | Sales |
| Revenue YTD | `TOTALYTD(Revenue, Calendar[date])` | CFO |
| BRL per USD | EUR/BRL ÷ EUR/USD cross-rate | Finance |
| Avg Max Temperature | `AVERAGE(temp_max)` | Analyst |

**Deferred metrics** (reviews table not yet in Gold schema): Average Review Score, 5-Star Review Rate.

---

## 8. Suggested Report Pages

| Page | Audience | Key visuals |
|---|---|---|
| Executive Overview | CEO, CFO | KPI cards (Revenue, Orders, AOV, Freight %), monthly revenue line, top-10 categories bar, Brazil filled map by customer state |
| Geography | Regional Managers | Side-by-side filled maps (demand vs supply), matrix by region, delivery days vs freight scatter |
| Product Categories | Catalogue, Merchandising | Treemap by revenue, dual-axis bar (revenue vs items), price vs freight scatter |
| Delivery & Operations | Ops, Logistics | Delivery days histogram, actual vs estimated trend, performance by seller state |
| Weather Impact Explorer | Analyst, Marketing | Scatter: temp_max vs revenue by category, precipitation vs orders line, revenue by weather code (labelled with WMO descriptions) |

---

## 9. Model Verification Checklist

Run through this list after building the model and before publishing to Power BI Service.

### Connection
- [ ] Npgsql 6.0.x is installed; version confirmed in Windows Registry or GAC
- [ ] `powerbi_reader` role exists with SELECT-only access; no INSERT/UPDATE/DELETE rows in `information_schema.role_table_grants`
- [ ] Power BI connects with `options=-csearch_path=analytics` in advanced connection field
- [ ] All 8 analytics tables load without errors in the Navigator

### Table naming
- [ ] All 8 tables renamed to display names (Section 3.1)
- [ ] `_Measures` disconnected table created and placed in model

### dim_date setup
- [ ] `Date` column (type: Date) added in Power Query from YYYYMMDD integer (Section 3.4)
- [ ] `Calendar` marked as date table using the `Date` column
- [ ] Date table validation passes (no gaps, no duplicates in date range)
- [ ] `date_key` integer column hidden from report view

### Relationships
- [ ] R1–R7 created and active (Section 3.2 table)
- [ ] R8: `Exchange Rates[base_currency_key]` → `Currencies[currency_key]` active
- [ ] R9: `Exchange Rates[quote_currency_key]` → `Currencies (Quote)[currency_key]` active — duplicate Currencies query created
- [ ] No active relationships detected between tables not in the R1–R9 list
- [ ] Auto-detected relationships reviewed and any incorrect ones deleted
- [ ] Cross-filter direction is Single on all relationships (no Bidirectional)

### Column hiding
- [ ] All surrogate keys hidden (Section 3.5 list)
- [ ] All `_loaded_at` columns hidden
- [ ] All source-system ID columns hidden

### DAX measures
- [ ] All stubs from Section 4.1 created in `_Measures` table
- [ ] `Total Revenue` returns a reasonable number (> 1 M BRL for full dataset)
- [ ] `Total Orders` returns ~99,000–100,000 for full dataset
- [ ] `Revenue MoM %` returns BLANK for the earliest month (Sep 2016) — correct
- [ ] `Revenue YTD` resets to zero at Jan 1 of each year
- [ ] `BRL per USD` returns a positive number in the range 3–5 (historically correct)

### Model view
- [ ] Model view resembles a clean star schema — no crossed relationship lines crossing through unrelated tables
- [ ] `_Measures` table visually separated from data tables
- [ ] `Currencies (Quote)` table clearly labelled and separated from primary `Currencies`

### Data validation
- [ ] Spot-check: Total Revenue for 2017 is between R$7 M and R$12 M (Olist full dataset range)
- [ ] Spot-check: fact_weather_daily rows visible for São Paulo (city="sao paulo", state="SP")
- [ ] Spot-check: Exchange Rates table has rows for EUR/BRL and EUR/USD pairs
- [ ] Calendar table covers the full order date range (Sep 2016 – Oct 2018 minimum)
- [ ] Freight % of Revenue is between 10% and 25% (expected range for Brazilian e-commerce)

### Publishing readiness
- [ ] Report file saved as `.pbix`
- [ ] Connection credentials not embedded in file (use Power BI Service gateway / credentials store)
- [ ] RLS roles defined if regional access control is required
- [ ] Scheduled refresh configured for the `powerbi_reader` account in Power BI Service
