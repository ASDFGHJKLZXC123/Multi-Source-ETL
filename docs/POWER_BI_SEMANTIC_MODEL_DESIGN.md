# Power BI Semantic Model Design — Multi-Source ETL Analytics

> **⚠ Partially aspirational.** Sections that reference `review_score`,
> `Average Review Score`, `5-Star Review Rate`, or other review-derived
> measures describe a target state — they are not deliverable today
> because review data is Bronze-only (no Silver/Gold layer; see the
> Roadmap in `README.md`). FX-rate sections that derive `BRL/USD` as
> `EUR/BRL ÷ EUR/USD` cross-rates are also stale: the pipeline now
> fetches the direct USD/BRL pair via `src/extract/extract_fx.py` and
> exposes the pre-joined view `analytics.v_sales_usd`.

**Project:** Multi-Source ETL Pipeline — Brazilian E-Commerce Analytics  
**Data Source:** PostgreSQL `analytics` schema (Star schema)  
**Target BI Tool:** Power BI Desktop + Service  
**Dataset Size:** 100K–500K rows | Daily refresh via ETL pipeline  
**Document Version:** 1.0  
**Last Updated:** 2026-04-15

---

## Executive Summary

This document specifies the Power BI semantic model architecture for the Brazilian E-Commerce analytics pipeline. The model ingests a normalized star schema from PostgreSQL (`analytics` schema) and surfaces it as interactive, self-service dashboards. Key design decisions cover storage modes (Import vs. DirectQuery), table relationships, DAX calculations, date handling, and layout organization for maximum usability and performance.

**Key Outcomes:**
- **6 dimension tables** + **3 fact tables** + **1 measures table**
- **Import mode** for all tables (fast, responsive, cost-effective refresh)
- **Role-playing relationships** to handle `dim_currency` as both base and quote currency
- **Degenerate dimensions** in `fact_weather_daily` (city, state) stored as columns, not separate tables
- **Nullable foreign keys** on `fact_sales` handled via DAX `IFERROR` patterns
- **Single star schema in Model view** with clear visual layout for stakeholder confidence

---

## 1. Import vs. DirectQuery Decision

### Recommendation: IMPORT MODE (All Tables)

#### Rationale

| Factor | Analysis | Decision |
|--------|----------|----------|
| **Data Volume** | 100K–500K rows total; largest fact table (~500K rows) well within Import memory | Import ✓ |
| **Query Latency** | Analytics queries need <2s response for interactive dashboards; Import cached memory << DirectQuery network roundtrip | Import ✓ |
| **Refresh Frequency** | Daily ETL pipeline refresh at scheduled time (no real-time requirement); Import handles daily refresh easily | Import ✓ |
| **Development Velocity** | Import allows offline dashboard development without DB access; DirectQuery requires live DB connection throughout | Import ✓ |
| **User Concurrency** | Estimated 20–50 concurrent report viewers; Import through Power BI Service scales efficiently without DB load pressure | Import ✓ |
| **Cost** | Import data in Power BI Service incurs per-GB monthly capacity cost (~$50–150 for this dataset); DirectQuery incurs per-query DB cost; Import is cheaper long-term | Import ✓ |
| **Incremental Load** | Daily refresh with 10K–50K new rows; Power BI incremental refresh policy supports daily partition strategy | Import + Incremental ✓ |
| **Data Freshness SLA** | Reports refreshed daily at 08:00 UTC is sufficient (not hourly); Import aligned with daily ETL pipeline schedule | Import ✓ |

#### Implementation Strategy

1. **All tables in Import mode** by default
2. **Incremental refresh policy** on fact tables (partition on `_loaded_at` → daily)
3. **Scheduled refresh** at 08:30 UTC daily (30 minutes after ETL pipeline completes)
4. **Query folding** not applicable to Import (data already cached)
5. **Optional future: DirectQuery on archival/historical fact tables** if size exceeds 2GB (not applicable now)

#### Storage Breakdown (Estimated)

| Table | Rows | Size (Memory) | Mode |
|-------|------|---------------|------|
| `fact_sales` | 500K | 45 MB | Import + Incremental |
| `fact_weather_daily` | 20K | 2 MB | Import |
| `fact_fx_rates` | 550 | 100 KB | Import |
| `dim_date` | 730 | 80 KB | Import |
| `dim_customer` | 100K | 8 MB | Import |
| `dim_product` | 33K | 3 MB | Import |
| `dim_store` | 3K | 300 KB | Import |
| `dim_currency` | 5 | 10 KB | Import |
| **Measures table** | 0 | 10 KB | Import |
| **Total** | **~650K** | **~58 MB** | **Import** |

**Power BI Service Capacity:** 1 GB per Premium Capacity unit; this model uses ~60 MB (< 1% of one unit).

---

## 2. Relationship Design

### Overview

The analytics star schema defines a clean dimensional model with:
- **6 dimension tables** (date, customer, product, store, currency, degenerate dims in facts)
- **3 fact tables** (sales, weather, FX rates)
- **8 relationships** (plus 2 role-playing relationships on dim_currency)

All relationships are one-to-many (dimension PK → fact FK) with correct cardinality and cross-filter directions.

### Relationship Specification

#### 2.1 Core Fact-to-Dimension Relationships

| Relationship ID | From Table | To Table | From Column | To Column | Cardinality | Cross-Filter | Active | Notes |
|---|---|---|---|---|---|---|---|---|
| R1 | fact_sales | dim_date | date_key | date_key | Many-to-One | Both | Yes | Order date dimension |
| R2 | fact_sales | dim_customer | customer_key | customer_key | Many-to-One | Both | Yes | Nullable FK: IFERROR handling |
| R3 | fact_sales | dim_product | product_key | product_key | Many-to-One | Both | Yes | Nullable FK: IFERROR handling |
| R4 | fact_sales | dim_store | store_key | store_key | Many-to-One | Both | Yes | Nullable FK: IFERROR handling |
| R5 | fact_weather_daily | dim_date | date_key | date_key | Many-to-One | Both | Yes | Weather observation date |
| R6 | fact_fx_rates | dim_date | date_key | date_key | Many-to-One | Both | Yes | FX rate date |
| R7 | fact_fx_rates | dim_currency (Base) | base_currency_key | currency_key | Many-to-One | Both | Yes | Role-playing: base currency |
| R8 | fact_fx_rates | dim_currency (Quote) | quote_currency_key | currency_key | Many-to-One | Both | No | Role-playing: quote currency (disabled to prevent ambiguity) |

#### 2.2 Role-Playing Relationship: dim_currency

**Problem:** `fact_fx_rates` has two foreign keys to `dim_currency`:
- `base_currency_key` (e.g., USD)
- `quote_currency_key` (e.g., BRL)

In a relational model, both keys point to the same `dim_currency` table. Power BI can only activate ONE relationship at a time per table pair.

**Solution: Hidden Role-Playing Dimension**

1. **Create two logical copies of `dim_currency` in the model view** (visual trick):
   - `dim_currency` (active) — used for base currency relationship
   - `dim_currency_Quote` (hidden copy, actually same table) — used for quote currency relationship

2. **Implementation Steps:**
   - Import `dim_currency` from PostgreSQL as normal → **`dim_currency`** table
   - Create a **duplicate reference table** in Power BI using DAX:
     ```dax
     dim_currency_Quote = dim_currency
     ```
   - Relationship R7 (active): `fact_fx_rates[base_currency_key]` → `dim_currency[currency_key]`
   - Relationship R8 (inactive, then redirect to duplicate): `fact_fx_rates[quote_currency_key]` → `dim_currency_Quote[currency_key]`
   - Set R8 to **active** (only one relationship per fact-dimension pair is active)
   - Hide `dim_currency_Quote` from report view (kept for internal reference only)

3. **DAX Pattern for Role-Playing Lookups:**
   ```dax
   BaseCurrencyName = RELATED(dim_currency[currency_name])
   QuoteCurrencyName = RELATED(dim_currency_Quote[currency_name])
   ```

**Why This Works:**
- Power BI sees two separate dimension tables (one hidden)
- Each relationship is one-to-many with clear cardinality
- DAX measures can use `RELATED()` to pull attributes from either role

---

#### 2.3 Degenerate Dimensions in fact_weather_daily

**Problem:** `fact_weather_daily` has no foreign keys to `dim_customer`, `dim_product`, or `dim_store`. Instead, it contains:
- `city` (text, degenerate dimension)
- `state` (text, degenerate dimension)

These are NOT separate dimensions because:
1. They repeat across many weather records (daily observations per city)
2. They lack primary keys or unique identifiers in the fact table
3. Creating a full `dim_geography` would be artificial and add schema complexity

**Solution: Keep Degenerate Dimensions as Fact Columns**

1. **Do NOT create a separate `dim_geography` table** (avoid false normalization)
2. **Store `city` and `state` as regular columns in `fact_weather_daily`**
3. **Make them available for filtering and grouping** in Power BI:
   - Add these columns to the Model with visibility enabled
   - Create a **measure group** for weather analytics
   - Users can slice weather data by city/state without a separate lookup

**DAX Pattern for Degenerate Dimension Aggregations:**
```dax
AvgTempByCity = 
CALCULATE(
    AVERAGE(fact_weather_daily[temp_max]),
    ALLEXCEPT(fact_weather_daily, fact_weather_daily[city])
)
```

---

#### 2.4 Nullable Foreign Keys on fact_sales

**Problem:** `fact_sales` has three nullable foreign keys:
- `customer_key` (NULL if order placed by anonymous guest or data quality issue)
- `product_key` (NULL if product was delisted or data missing)
- `store_key` (NULL if seller removed or data missing)

If a FK is NULL, Power BI's relationship cannot match the row to the dimension. This can cause:
- Missing/blank dimension attributes in reports
- Inflated "Unknown" or NULL categories

**Solution: IFERROR DAX Pattern + Nullable FK Handling**

1. **Define relationships as normal** (even though some FKs are NULL)
   - Cardinality: `Many-to-One`
   - Assume one-to-one matching where FK ≠ NULL
   - Cross-filter: `Both` (dimension filters fact, and vice versa)

2. **In measures, wrap RELATED() calls with IFERROR():**
   ```dax
   CustomerName = IFERROR(RELATED(dim_customer[customer_code]), "Unknown")
   ProductCategory = IFERROR(RELATED(dim_product[product_category_name_english]), "Uncategorized")
   StoreName = IFERROR(RELATED(dim_store[store_id]), "Unknown Store")
   ```

3. **Alternative: Add a "Unknown" row to each dimension**
   - Prepend a dummy row to `dim_customer` with `customer_key = 0`, `customer_code = "UNKNOWN"`
   - Prepend dummy rows to `dim_product` and `dim_store`
   - In the ETL, set NULL FKs to 0 instead
   - Relationships will match NULL replacements to the dummy rows
   - **Pros:** No IFERROR needed; cleaner visuals
   - **Cons:** Requires ETL change; uses one row per dimension

   **Recommendation:** Use IFERROR pattern (non-invasive, no ETL change).

---

### Relationship Cardinality Summary

```
dim_date (1) ──M── fact_sales (M)
dim_date (1) ──M── fact_weather_daily (M)
dim_date (1) ──M── fact_fx_rates (M)

dim_customer (1) ──M── fact_sales (M)  [nullable FK, handled by IFERROR]
dim_product (1) ──M── fact_sales (M)   [nullable FK, handled by IFERROR]
dim_store (1) ──M── fact_sales (M)     [nullable FK, handled by IFERROR]

dim_currency (1) ──M── fact_fx_rates (M) [base_currency_key, active relationship]
dim_currency_Quote (1) ──M── fact_fx_rates (M) [quote_currency_key, role-playing]
```

---

## 3. _Measures Table (Placeholder / Calculation Layer)

### Overview

The `_Measures` table is a **hidden, virtual table** (zero rows, DAX-only) that stores all business logic measures and KPIs. This table:
- Keeps the model organized (measures grouped in one place in the fields panel)
- Separates calculation logic from data tables
- Supports cascading measure dependencies
- Is never included in visuals directly (only measures are used)

### Creation Steps

1. **In Power BI Desktop, go to Model view**
2. **Create a new blank table:**
   ```dax
   _Measures = SELECTCOLUMNS(ADDCOLUMNS(ROW(), "_", 0), "_")
   ```
   This creates a single-row, single-column table (purely symbolic).

3. **Mark as Hidden:**
   - Right-click table → **Table Properties** → Deselect "Show in Model view"
   - The table is invisible in Model view but rows appear in Fields panel (measures only)

4. **Add measures to this table** (see section 3.1 below)

---

### 3.1 Core Measure Definitions

#### 3.1.1 Total Revenue (BRL and USD variants)

```dax
[Total Revenue BRL] =
SUMX(fact_sales, fact_sales[unit_price] * fact_sales[quantity] + fact_sales[freight_value])
```

**Usage:** Revenue aggregated across all orders, sliceable by date, customer, product, store.

```dax
[Total Revenue USD] =
CALCULATE(
    [Total Revenue BRL],
    ADDCOLUMNS(fact_sales, "fx_rate", <calculate average BRL/USD rate for period>)
)
* AVERAGE(fact_fx_rates[rate])
```

**Note:** This is a simplified approximation. For precise USD conversion, ETL should pre-compute FX-adjusted prices; this measure assumes a single exchange rate per reporting period.

---

#### 3.1.2 Total Orders

```dax
[Total Orders] =
DISTINCTCOUNT(fact_sales[order_item_id])
```

**Usage:** Count distinct orders (not order items, to avoid double-counting). Since `fact_sales` has one row per order item, we count distinct order_item_id.

**Alternative (if order-level granularity exists):**
```dax
[Total Orders (Distinct)] =
COUNTA(DISTINCT(fact_sales[order_id]))
```

---

#### 3.1.3 Average Order Value (AOV)

```dax
[Average Order Value] =
DIVIDE(
    [Total Revenue BRL],
    [Total Orders],
    0
)
```

**Usage:** Revenue per order; commonly sliced by customer segment, region, or time period.

---

#### 3.1.4 Freight Rate % (Freight as % of Revenue)

```dax
[Freight Rate %] =
DIVIDE(
    SUM(fact_sales[freight_value]),
    [Total Revenue BRL],
    0
)
```

**Usage:** Identifies orders with high shipping costs relative to product price; useful for logistics optimization.

---

#### 3.1.5 Average Review Score

```dax
[Average Review Score] =
AVERAGE(fact_sales[review_score])
```

**Usage:** Customer satisfaction KPI; filter by order date, product category, or store to identify quality issues.

**Note:** `review_score` is nullable in the source data. AVERAGE() ignores NULLs; use `AVERAGEX()` for custom handling:

```dax
[Average Review Score (Explicit)] =
AVERAGEX(
    FILTER(fact_sales, NOT(ISBLANK(fact_sales[review_score]))),
    fact_sales[review_score]
)
```

---

#### 3.1.6 Currency Rate Change % (Base vs Quote)

```dax
[FX Rate Change %] =
VAR EarliestRate = MINX(ALLSELECTED(fact_fx_rates), fact_fx_rates[rate])
VAR LatestRate = MAXX(ALLSELECTED(fact_fx_rates), fact_fx_rates[rate])
RETURN
    DIVIDE(LatestRate - EarliestRate, EarliestRate, 0)
```

**Usage:** Shows USD/BRL volatility over selected time period; useful for understanding revenue impact of currency fluctuations.

---

#### 3.1.7 Temperature Range (Daily)

```dax
[Temp Range Max - Min] =
SUMX(fact_weather_daily, fact_weather_daily[temp_max] - fact_weather_daily[temp_min])
```

**Usage:** Aggregates daily temperature variance; can be sliced by city or week.

```dax
[Avg Temp Max] =
AVERAGE(fact_weather_daily[temp_max])

[Avg Temp Min] =
AVERAGE(fact_weather_daily[temp_min])

[Avg Temp Range] =
[Avg Temp Max] - [Avg Temp Min]
```

---

### 3.2 Advanced Measure Patterns

#### Time-Intelligence Measures

```dax
[Total Revenue YTD] =
TOTALYTD([Total Revenue BRL], dim_date[date])

[Total Revenue MoM] =
VAR CurrentMonth = [Total Revenue BRL]
VAR PreviousMonth = CALCULATE([Total Revenue BRL], DATEADD(dim_date[date], -1, MONTH))
RETURN
    DIVIDE(CurrentMonth - PreviousMonth, PreviousMonth, 0)
```

#### Conditional Aggregation

```dax
[High-Value Orders] =
SUMX(FILTER(fact_sales, fact_sales[unit_price] > 500), fact_sales[unit_price] * fact_sales[quantity])

[Late Deliveries] =
COUNTX(FILTER(fact_sales, fact_sales[delivery_days_actual] > fact_sales[delivery_days_estimated]), fact_sales[order_item_id])
```

#### Measure Hiding

Hide intermediate measures used only in other calculations:
- Prefix with underscore: `_TotalRevenueBefore` (hidden from end users)
- Visible measures: `Total Revenue BRL`, `Total Orders` (public KPIs)

---

## 4. dim_date as Power BI Date Table

### Why Mark as Date Table?

Power BI's **Time Intelligence functions** (YTD, QTD, MoM, YoY comparisons) require a designated date table. Without this:
- `TOTALYTD()`, `DATESYTD()`, and similar functions fail or return incorrect results
- Time hierarchies (Year → Quarter → Month → Day) are not automatically generated
- Slicers lose smart filtering capabilities

### Configuration Steps

#### Step 1: Open Model View

1. In Power BI Desktop, click **Model** (left sidebar)
2. Select the `dim_date` table (click on the table name)

#### Step 2: Mark as Date Table

1. Right-click the `dim_date` table
2. Select **Mark as date table**
3. In the dialog, confirm:
   - **Date column:** `dim_date[date]` (PK column, DATE or DATETIME type)
   - Click **OK**

**UI Path:** Ribbon → **Table Design** → **Mark as Date Table** (if table already selected)

#### Step 3: Verify Configuration

1. The `dim_date` table icon should change (small calendar icon appears)
2. In Fields panel, `dim_date[date]` is now the designated date column
3. Time intelligence functions are now available in DAX formulas

### Power BI Date Table Validation

Ensure `dim_date` meets requirements:

| Requirement | Status | Notes |
|---|---|---|
| **Column Name** | `date` | Should be DATE or DATETIME type |
| **Uniqueness** | PK on `date_key` (INT YYYYMMDD) | One row per calendar day |
| **Continuity** | Contiguous (2016-01-01 to 2026-12-31) | No gaps; Power BI expects unbroken sequence |
| **Type** | DATE or DATETIME | Not integer; Power BI recognizes date semantics |
| **Range** | Covers all fact table dates | Must span from earliest to latest order date |

If `dim_date[date]` is stored as TEXT in PostgreSQL, convert it in Power BI:

```dax
Date = DATE(LEFT([date], 4), MID([date], 6, 2), RIGHT([date], 2))
```

Or use the **Data Type** selector in Data view to set column type to **Date**.

---

## 5. Model View Layout & Organization

### Goal

Create a **clean, visually intuitive star schema** in Power BI Model view that:
- Immediately communicates the data relationships to stakeholders
- Separates dimensions from facts
- Shows role-playing relationships clearly
- Minimizes crossing lines (visual clutter)

### Layout Strategy

```
┌─────────────────────────────────────────────────────────┐
│ Power BI Model View — Logical Organization             │
└─────────────────────────────────────────────────────────┘

┌──────────────┐
│  dim_date    │   ← Central date dimension
│  (230 rows)  │      Connects to all facts
└──────┬───────┘
       │
   ┌───┴──────┬───────────┬──────────────┐
   │          │           │              │
   v          v           v              v
┌─────────┐ ┌──────────────┐ ┌────────────┐
│fact_    │ │fact_weather_ │ │ fact_fx_   │
│sales    │ │daily         │ │ rates      │
│(500K)   │ │(20K rows)    │ │(550 rows)  │
└────┬────┘ └──────┬───────┘ └──────┬─────┘
     │             │                 │
     ├─────────────┴─────────────────┤
     │                               │
     v                               v
┌────────────┐  ┌────────────┐  ┌──────────┐
│dim_customer│  │dim_product │  │dim_store │
│(100K rows) │  │(33K rows)  │  │(3K rows) │
└────────────┘  └────────────┘  └──────────┘

    ┌─────────────────────────────────┐
    │    dim_currency (5 rows)        │
    │  [Hidden copy for role-playing] │
    │    dim_currency_Quote           │
    └─────────────────────────────────┘
         (Connects via fact_fx_rates)

┌───────────────────────────────────────┐
│  _Measures (hidden, 0 rows)           │
│  [All DAX calculations reside here]   │
└───────────────────────────────────────┘
```

### Positioning Rules

#### Zone 1: Center — Date Dimension
- Place `dim_date` in the **center-top** of the model view
- It connects to all three fact tables (central hub)
- Use the date dimension as the visual anchor

#### Zone 2: Right — Fact Tables
- Arrange fact tables **horizontally below dim_date**:
  - **Left:** `fact_sales` (primary, largest, most queries)
  - **Center:** `fact_weather_daily` (enrichment layer)
  - **Right:** `fact_fx_rates` (secondary, small)
- Spread them left-to-right to minimize crossing lines

#### Zone 3: Bottom-Left — Sales Dimensions
- Arrange lookup dimensions **below fact_sales**:
  - `dim_customer` (leftmost)
  - `dim_product` (center)
  - `dim_store` (rightmost)
- These form the classic star around `fact_sales`

#### Zone 4: Bottom-Right — Currency Dimension
- Place `dim_currency` and `dim_currency_Quote` (hidden) **bottom-right**
- Near `fact_fx_rates` to show the role-playing relationship clearly

#### Zone 5: Top-Right — Measures
- Place `_Measures` table (hidden) in **top-right corner**
- Out of the way (hidden) but logically grouped with dimensions

### Visual Relationship Annotations

When you see relationship lines in Model view:
- **Solid line:** Active relationship (used by default in measures)
- **Dashed line:** Inactive relationship (requires explicit USERELATIONSHIP in DAX)
- **Color coding:** Same color = related fact/dimension pair

**Example:** fact_fx_rates has two relationships to dim_currency:
- R7 (base_currency_key): Solid line to `dim_currency` (active)
- R8 (quote_currency_key): Dashed line to `dim_currency_Quote` (inactive, role-playing)

---

## 6. Column Hiding & Visibility Guidance

### Why Hide Columns?

- **Reduces confusion:** End users see only business-meaningful columns
- **Simplifies field panel:** 100+ columns visible = overwhelming
- **Enforces governance:** Measures (not raw columns) define data lineage
- **Prevents misuse:** Hides surrogate keys that would produce incorrect aggregations

### Hide These Columns

| Column(s) | Table | Reason | Visibility |
|---|---|---|---|
| `_loaded_at` | All tables | Audit/technical metadata; not for reporting | Hide |
| `customer_key` | `fact_sales` | Surrogate key; users don't understand numeric IDs | Hide |
| `product_key` | `fact_sales` | Surrogate key for internal joins only | Hide |
| `store_key` | `fact_sales` | Surrogate key for internal joins only | Hide |
| `date_key` | `fact_sales`, `fact_weather_daily`, `fact_fx_rates` | Surrogate key; use dim_date[date] instead | Hide |
| `date_key` | `dim_date` | Internal join key; use date[date] for slicing | Hide |
| `currency_key` | `dim_currency`, `dim_currency_Quote` | Surrogate key; use currency_code/currency_name | Hide |
| `base_currency_key` | `fact_fx_rates` | FK; not intended for end-user queries | Hide |
| `quote_currency_key` | `fact_fx_rates` | FK; not intended for end-user queries | Hide |
| `customer_id` | `dim_customer` | Source business key; customer_code is preferred | Hide (optional) |
| `product_id` | `dim_product` | Source business key; product_category_name is primary | Hide (optional) |
| `store_id` | `dim_store` | Source business key; state/zip_code_prefix for analysis | Hide (optional) |

### Show These Columns (User-Facing)

| Column(s) | Table | Reason |
|---|---|---|
| `date` | `dim_date` | Date slicer, time-based filtering |
| `year`, `quarter`, `month`, `day_of_week` | `dim_date` | Time hierarchies (Year → QTR → Month → Day) |
| `is_weekend`, `is_holiday` | `dim_date` | Business logic filtering (exclude weekends, include holidays) |
| `customer_code` | `dim_customer` | Customer identifier for end users |
| `city`, `state` | `dim_customer` | Geographic filtering (customer location) |
| `is_active` | `dim_customer` | Filter active vs. inactive customers |
| `product_category_name` | `dim_product` | Category filtering (electronics, home, etc.) |
| `product_category_name_english` | `dim_product` | English version for multi-lingual reports |
| `weight_g`, `length_cm`, `height_cm`, `width_cm` | `dim_product` | Product size attributes |
| `state`, `zip_code_prefix` | `dim_store` | Store location filtering |
| `is_active` | `dim_store` | Filter active/inactive sellers |
| `currency_code`, `currency_name` | `dim_currency` | FX rate reference; currency identification |
| `unit_price`, `quantity`, `freight_value` | `fact_sales` | Detail-level metrics (allow drill-down) |
| `review_score` | `fact_sales` | Customer satisfaction metric |
| `temp_max`, `temp_min`, `precipitation_sum`, `windspeed_max`, `weathercode` | `fact_weather_daily` | Weather attributes for correlation analysis |
| `city`, `state` | `fact_weather_daily` | Weather location matching (degenerate dims) |
| `rate` | `fact_fx_rates` | FX rate value for currency conversion |

### Hidden Tables

| Table | Reason | Notes |
|---|---|---|
| `_Measures` | Placeholder for DAX formulas | Users interact with individual measures, not the table |
| `dim_currency_Quote` | Role-playing dimension copy | Internal use only; avoid confusion with main dim_currency |

---

## 7. DAX Patterns & Advanced Considerations

### 7.1 Handling Nullable Foreign Keys

**Problem:** When `fact_sales[customer_key]` is NULL, `RELATED(dim_customer[...])` returns BLANK.

**Solution A: IFERROR Pattern**
```dax
Customer Name = IFERROR(RELATED(dim_customer[customer_code]), "Unknown")
```

**Solution B: COALESCE Pattern**
```dax
Customer State = 
COALESCE(
    RELATED(dim_customer[state]),
    "Unknown"
)
```

**Solution C: Explicit FK Check**
```dax
Has Customer = NOT(ISBLANK(fact_sales[customer_key]))
```

Then use in filters:
```dax
Orders with Customer = COUNTX(FILTER(fact_sales, [Has Customer]), fact_sales[order_item_id])
```

---

### 7.2 Time Intelligence Measures

Once `dim_date` is marked as the date table, use:

```dax
[Revenue YTD] = TOTALYTD([Total Revenue BRL], dim_date[date])

[Revenue MTD] = 
CALCULATE(
    [Total Revenue BRL],
    DATESMTD(dim_date[date])
)

[Revenue QTD] =
CALCULATE(
    [Total Revenue BRL],
    DATESQTD(dim_date[date])
)

[Revenue PY] = 
CALCULATE(
    [Total Revenue BRL],
    DATEADD(dim_date[date], -1, YEAR)
)

[Revenue YoY %] =
DIVIDE(
    [Total Revenue BRL],
    CALCULATE([Total Revenue BRL], DATEADD(dim_date[date], -1, YEAR)),
    0
)
```

---

### 7.3 Degenerate Dimension Aggregations (fact_weather_daily)

Since `city` and `state` are stored as columns in `fact_weather_daily` (not separate dimensions):

```dax
[Avg Temp by City] =
CALCULATE(
    AVERAGE(fact_weather_daily[temp_max]),
    ALLEXCEPT(fact_weather_daily, fact_weather_daily[city])
)

[Max Windspeed by State] =
CALCULATE(
    MAX(fact_weather_daily[windspeed_max]),
    ALLEXCEPT(fact_weather_daily, fact_weather_daily[state])
)

[Rainy Days Count] =
SUMX(
    FILTER(
        fact_weather_daily,
        fact_weather_daily[precipitation_sum] > 0
    ),
    1
)
```

---

### 7.4 Role-Playing Currency Conversion

To join `fact_fx_rates` and pull both base and quote currency names:

```dax
Base Currency = 
RELATED(dim_currency[currency_name])
   -- Uses active relationship (R7: base_currency_key)

Quote Currency = 
RELATED(dim_currency_Quote[currency_name])
   -- Uses role-playing relationship (R8: quote_currency_key, dim_currency_Quote)

Currency Pair = 
[Base Currency] & "/" & [Quote Currency]
   -- Example output: "USD/BRL"
```

---

## 8. Implementation Checklist

### Phase 1: Data Import & Schema Setup

- [ ] Export `analytics` schema tables from PostgreSQL to CSV or directly connect to PostgreSQL
  - [ ] `dim_date` (730 rows)
  - [ ] `dim_customer` (100K rows)
  - [ ] `dim_product` (33K rows)
  - [ ] `dim_store` (3K rows)
  - [ ] `dim_currency` (5 rows)
  - [ ] `fact_sales` (500K rows)
  - [ ] `fact_weather_daily` (20K rows)
  - [ ] `fact_fx_rates` (550 rows)

- [ ] In Power BI Desktop:
  - [ ] **Get Data** → PostgreSQL OR Excel/CSV
  - [ ] Load all 8 tables into the model
  - [ ] Verify row counts match source

- [ ] Data Type Validation:
  - [ ] `dim_date[date]` is DATE type (not text)
  - [ ] All FK columns are INT or BIGINT
  - [ ] All measure columns are DECIMAL or numeric
  - [ ] `_loaded_at` is DATETIME (for auditing)

---

### Phase 2: Relationship Configuration

- [ ] **Create relationships** in Model view:
  - [ ] R1: `fact_sales[date_key]` → `dim_date[date_key]` (Many-to-One, Both)
  - [ ] R2: `fact_sales[customer_key]` → `dim_customer[customer_key]` (Many-to-One, Both)
  - [ ] R3: `fact_sales[product_key]` → `dim_product[product_key]` (Many-to-One, Both)
  - [ ] R4: `fact_sales[store_key]` → `dim_store[store_key]` (Many-to-One, Both)
  - [ ] R5: `fact_weather_daily[date_key]` → `dim_date[date_key]` (Many-to-One, Both)
  - [ ] R6: `fact_fx_rates[date_key]` → `dim_date[date_key]` (Many-to-One, Both)
  - [ ] R7: `fact_fx_rates[base_currency_key]` → `dim_currency[currency_key]` (Many-to-One, Both, **Active**)

- [ ] **Configure role-playing relationship:**
  - [ ] Create duplicate table: `dim_currency_Quote = dim_currency`
  - [ ] Hide `dim_currency_Quote` from Model view
  - [ ] Create R8: `fact_fx_rates[quote_currency_key]` → `dim_currency_Quote[currency_key]` (Many-to-One, Both, **Active**)

- [ ] **Verify relationship cardinality:**
  - [ ] No *-to-* or circular relationships
  - [ ] All Many-to-One relationships are correct
  - [ ] No warnings in Model view (red triangle warnings)

---

### Phase 3: Date Table Configuration

- [ ] Mark `dim_date` as date table:
  - [ ] Right-click `dim_date` → **Mark as date table**
  - [ ] Select `dim_date[date]` as the date column
  - [ ] Verify calendar icon appears on `dim_date` table

- [ ] Verify date continuity:
  - [ ] Min date: 2016-01-01 (or earlier)
  - [ ] Max date: 2026-12-31 (or later, covering all facts)
  - [ ] No gaps in calendar

- [ ] Create time hierarchies (optional, for ease of use):
  - [ ] In Model view, select `dim_date`
  - [ ] **Table Design** → **New Hierarchy**
  - [ ] Hierarchy: `Year` → `Quarter` → `Month` → `Date`

---

### Phase 4: Measures & Calculations

- [ ] **Create `_Measures` placeholder table:**
  ```dax
  _Measures = SELECTCOLUMNS(ADDCOLUMNS(ROW(), "_", 0), "_")
  ```
  - [ ] Hide from Model view
  - [ ] Hide from Report view

- [ ] **Add measures to `_Measures` table:**
  - [ ] `[Total Revenue BRL]` — sum of unit_price + freight_value
  - [ ] `[Total Orders]` — distinct count of order items
  - [ ] `[Average Order Value]` — revenue / orders
  - [ ] `[Freight Rate %]` — freight / revenue
  - [ ] `[Average Review Score]` — avg of review_score (ignore NULLs)
  - [ ] `[FX Rate Change %]` — (latest - earliest) / earliest
  - [ ] `[Avg Temp Range]` — avg(temp_max - temp_min)

- [ ] **Test measures:**
  - [ ] Create a simple table visual with each measure
  - [ ] Verify values are reasonable (no #DIV/0! errors)
  - [ ] Spot-check manual calculations

---

### Phase 5: Column Visibility Configuration

- [ ] **Hide surrogate keys:**
  - [ ] `date_key` (all tables)
  - [ ] `customer_key`, `product_key`, `store_key` (`fact_sales`)
  - [ ] `currency_key` (`dim_currency`)
  - [ ] `base_currency_key`, `quote_currency_key` (`fact_fx_rates`)

- [ ] **Hide audit columns:**
  - [ ] `_loaded_at` (all tables)

- [ ] **Hide source business keys (optional):**
  - [ ] `customer_id` (dim_customer) — use `customer_code` instead
  - [ ] `product_id` (dim_product) — use `product_category_name` instead
  - [ ] `store_id` (dim_store) — use state/zip_code_prefix instead

- [ ] **Verify visibility:**
  - [ ] In Fields panel, users see only business columns
  - [ ] All measures appear under `_Measures`
  - [ ] Surrogate keys are hidden (not in Fields list)

---

### Phase 6: Model View Layout & Cleanup

- [ ] **Arrange tables in Model view:**
  - [ ] Center-top: `dim_date`
  - [ ] Horizontal row below: `fact_sales`, `fact_weather_daily`, `fact_fx_rates`
  - [ ] Below facts: `dim_customer`, `dim_product`, `dim_store`
  - [ ] Bottom-right: `dim_currency`, `dim_currency_Quote` (hidden)
  - [ ] Top-right: `_Measures` (hidden)

- [ ] **Verify relationship lines:**
  - [ ] No crossing lines (or minimal crossing)
  - [ ] All relationships clearly visible
  - [ ] Color-coded by related table pair

- [ ] **Document relationships:**
  - [ ] Create a relationship matrix (table in a notes document or visual)
  - [ ] Reference cardinality and cross-filter direction for each relationship

---

### Phase 7: Testing & Validation

- [ ] **End-to-end testing:**
  - [ ] Create test dashboard with slicers for date, customer, product, store
  - [ ] Filter by single customer → verify only that customer's orders show
  - [ ] Filter by product category → verify fact_sales filtered correctly
  - [ ] Filter by date range → verify all facts filtered (sales, weather, FX)

- [ ] **Nullable FK testing:**
  - [ ] Identify orders with NULL customer_key, product_key, or store_key
  - [ ] Verify IFERROR measures return "Unknown" (not blank)
  - [ ] Total with and without NULL FKs should reconcile

- [ ] **Role-playing relationship testing:**
  - [ ] Add `[Base Currency]` and `[Quote Currency]` to a visual
  - [ ] Verify both are pulled correctly from `fact_fx_rates`
  - [ ] Test filtering by currency (both base and quote should filter)

- [ ] **Time intelligence testing:**
  - [ ] Create a visual with `[Revenue YTD]`, `[Revenue MTD]`, `[Revenue MoM %]`
  - [ ] Verify values change as date slicer changes
  - [ ] Manually verify one month's calculation

---

### Phase 8: Refresh & Performance

- [ ] **Set up refresh schedule:**
  - [ ] Power BI Service → **Settings** → **Scheduled refresh**
  - [ ] Set to 08:30 UTC daily (30 min after ETL pipeline)
  - [ ] Configure incremental refresh on `fact_sales` (by `_loaded_at` date)

- [ ] **Monitor performance:**
  - [ ] Create a simple visual (card with `[Total Revenue BRL]`)
  - [ ] Measure query time: should be < 2 seconds
  - [ ] Check model size: should be < 100 MB in Power BI Service

- [ ] **Configure alerts (optional):**
  - [ ] Set refresh failure alerts (email if refresh fails)
  - [ ] Set data anomaly alerts (e.g., if revenue drops > 50% MoM)

---

### Phase 9: Documentation & Handoff

- [ ] **Create data dictionary:**
  - [ ] One row per column
  - [ ] Table name, column name, data type, description, business usage

- [ ] **Document assumptions:**
  - [ ] Daily refresh cadence (not real-time)
  - [ ] Nullable FK handling via IFERROR
  - [ ] Degenerate dimensions in fact_weather_daily
  - [ ] Role-playing dim_currency approach

- [ ] **Train end users:**
  - [ ] Demo: how to use slicers, drill-down
  - [ ] Demo: how to interpret measures (vs. raw columns)
  - [ ] FAQ: Why are some columns hidden? → "Enforces correct calculations"

- [ ] **Establish SLAs:**
  - [ ] Refresh schedule: Daily 08:30 UTC
  - [ ] Support contact: Data team
  - [ ] Known limitations: Real-time (not supported), historical data (2 years only)

---

## 9. Troubleshooting & Common Issues

### Issue 1: Circular Relationship Detected

**Symptom:** Power BI throws error "A circular relationship was detected."

**Cause:** Two or more relationships form a loop in the graph (e.g., A→B→C→A).

**Solution:**
- Disable one relationship (right-click → **Edit** → uncheck **Active**)
- Use `USERELATIONSHIP()` in DAX to explicitly reference inactive relationships
- For role-playing dimensions, this is expected; disable R8 (quote currency) and activate only when needed

---

### Issue 2: Relationships Show as Ambiguous

**Symptom:** Visual shows blank/error; DAX returns BLANK unexpectedly.

**Cause:** Cross-filter direction is wrong, or multiple paths exist to the same dimension.

**Solution:**
- Check relationship cross-filter direction: should be **Both** for star schema
- Verify no "diamond" relationships (multiple paths from fact to dimension)
- Use `USERELATIONSHIP()` in measures to disambiguate

---

### Issue 3: Nullable Foreign Keys Cause "Unknown" Inflation

**Symptom:** "Unknown" category dominates visual (e.g., 50% of orders show "Unknown" customer).

**Cause:** Many `customer_key` values are NULL in `fact_sales`.

**Solution:**
- Verify in source data: `SELECT COUNT(*) FROM fact_sales WHERE customer_key IS NULL;`
- If inflation is expected, document it in data dictionary
- Consider adding a "No Customer" dimension row for explicit handling (alternative to IFERROR)
- Create a measure that counts orders with/without customer for transparency:
  ```dax
  [Orders with Customer] = COUNTX(FILTER(fact_sales, NOT(ISBLANK(fact_sales[customer_key]))), fact_sales[order_item_id])
  [Orders without Customer] = [Total Orders] - [Orders with Customer]
  ```

---

### Issue 4: Time Intelligence Functions Return Blank

**Symptom:** `TOTALYTD()` or `DATESYTD()` returns BLANK; YTD measures are empty.

**Cause:** `dim_date` is not marked as the date table.

**Solution:**
- Go to Model view
- Right-click `dim_date` → **Mark as date table**
- Select `dim_date[date]` as the date column
- Retry measure: `TOTALYTD([Total Revenue BRL], dim_date[date])`

---

### Issue 5: FX Rate Conversion Shows Only Base or Quote Currency

**Symptom:** `[Quote Currency]` measure returns BLANK; only `[Base Currency]` shows.

**Cause:** Role-playing relationship R8 is not active, or `dim_currency_Quote` table doesn't exist.

**Solution:**
- Verify `dim_currency_Quote` table exists: `dim_currency_Quote = dim_currency`
- Verify R8 relationship is active: `fact_fx_rates[quote_currency_key]` → `dim_currency_Quote[currency_key]`
- Check cardinality is Many-to-One
- Test measure:
  ```dax
  Quote Currency = RELATED(dim_currency_Quote[currency_name])
  ```

---

### Issue 6: Query Takes > 10 Seconds (Performance Issue)

**Symptom:** Visual takes 10+ seconds to render; dashboard is sluggish.

**Cause:** 
- Import data hasn't been fully loaded (initial compression)
- Measure is inefficient (SUMX over all rows without context)
- Incorrect aggregation (no summarization happening)

**Solution:**
- Wait 2–3 minutes for initial model compression on first load
- Check measure DAX: ensure `SUMX()` is not iterating over entire fact table
- Use `CALCULATE()` with filters instead of iterative functions
- Verify indexes exist on FK columns in source PostgreSQL
- Check Power BI Desktop Task Manager: is CPU at 100%?

---

## 10. Appendix: Sample DAX Library

### Appendix A: Complete Measures Code

```dax
-- Revenue Measures
[Total Revenue BRL] =
SUMX(fact_sales, fact_sales[unit_price] * fact_sales[quantity] + fact_sales[freight_value])

[Total Freight BRL] =
SUM(fact_sales[freight_value])

[Avg Unit Price] =
AVERAGE(fact_sales[unit_price])

-- Order Metrics
[Total Orders] =
DISTINCTCOUNT(fact_sales[order_item_id])

[Avg Order Value] =
DIVIDE([Total Revenue BRL], [Total Orders], 0)

[Order Count by Customer] =
DISTINCTCOUNT(fact_sales[customer_key])

-- Freight Analysis
[Freight Rate %] =
DIVIDE([Total Freight BRL], [Total Revenue BRL], 0)

[Avg Freight per Order] =
DIVIDE([Total Freight BRL], [Total Orders], 0)

-- Customer Metrics
[Unique Customers] =
DISTINCTCOUNT(fact_sales[customer_key])

[Avg Orders per Customer] =
DIVIDE([Total Orders], [Unique Customers], 0)

[Repeat Customer Rate] =
VAR TotalCustomers = [Unique Customers]
VAR RepeatCustomers = SUMX(DISTINCT(fact_sales[customer_key]), CALCULATE(IF([Total Orders] > 1, 1, 0)))
RETURN DIVIDE(RepeatCustomers, TotalCustomers, 0)

-- Review Metrics
[Avg Review Score] =
AVERAGE(fact_sales[review_score])

[5-Star Reviews] =
COUNTX(FILTER(fact_sales, fact_sales[review_score] = 5), fact_sales[order_item_id])

[1-Star Reviews] =
COUNTX(FILTER(fact_sales, fact_sales[review_score] = 1), fact_sales[order_item_id])

-- Weather Metrics
[Avg Temp Max] =
AVERAGE(fact_weather_daily[temp_max])

[Avg Temp Min] =
AVERAGE(fact_weather_daily[temp_min])

[Avg Temp Range] =
[Avg Temp Max] - [Avg Temp Min]

[Rainy Days] =
COUNTX(FILTER(fact_weather_daily, fact_weather_daily[precipitation_sum] > 0), fact_weather_daily[date_key])

[Avg Windspeed] =
AVERAGE(fact_weather_daily[windspeed_max])

-- FX Metrics
[Latest FX Rate] =
MAXX(fact_fx_rates, fact_fx_rates[rate])

[Earliest FX Rate] =
MINX(fact_fx_rates, fact_fx_rates[rate])

[FX Rate Change %] =
DIVIDE([Latest FX Rate] - [Earliest FX Rate], [Earliest FX Rate], 0)

[Base Currency] =
RELATED(dim_currency[currency_name])

[Quote Currency] =
RELATED(dim_currency_Quote[currency_name])

[Currency Pair] =
[Base Currency] & "/" & [Quote Currency]

-- Time Intelligence (Requires dim_date marked as date table)
[Revenue YTD] =
TOTALYTD([Total Revenue BRL], dim_date[date])

[Revenue MTD] =
CALCULATE([Total Revenue BRL], DATESMTD(dim_date[date]))

[Revenue QTD] =
CALCULATE([Total Revenue BRL], DATESQTD(dim_date[date]))

[Revenue PY] =
CALCULATE([Total Revenue BRL], DATEADD(dim_date[date], -1, YEAR))

[Revenue YoY %] =
DIVIDE([Total Revenue BRL], [Revenue PY], 0)

[Revenue MoM %] =
VAR CurrentMonth = [Total Revenue BRL]
VAR PreviousMonth = CALCULATE([Total Revenue BRL], DATEADD(dim_date[date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PreviousMonth, PreviousMonth, 0)

-- Helper Measures
_IsCurrentYear = YEAR(TODAY()) = YEAR(MAX(dim_date[date]))
_SelectedDateRange = MAX(dim_date[date]) - MIN(dim_date[date])
```

---

## 11. Sign-Off & Approvals

| Role | Name | Approval Date | Notes |
|---|---|---|---|
| Data Analyst | (Your Name) | 2026-04-15 | Design document prepared |
| BI Architect | (Approval Pending) | — | Pending review |
| Data Engineer | (Approval Pending) | — | Pending ETL coordination |
| Business Stakeholder | (Approval Pending) | — | Pending requirements validation |

---

## Document Control

| Version | Date | Author | Changes |
|---|---|---|---|
| 1.0 | 2026-04-15 | Data Analyst | Initial design document |

---

**End of Design Document**
