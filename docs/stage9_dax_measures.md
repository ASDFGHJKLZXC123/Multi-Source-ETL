# Stage 9 — DAX Measures

> **Power BI model target:** `_Measures` disconnected table  
> **Currency:** All monetary measures are BRL unless explicitly labelled USD  
> **Calendar table:** `Calendar` (marked as date table on the `date` DATE column)  
> **Grain reminder:** `Sales Transactions` is one row per order line item. Use `DISTINCTCOUNT(order_code)` for order-level counts, `COUNTROWS` or `SUM(quantity)` for unit-level counts.

> **Column rename reference:** Several Power BI model column renames (defined in `docs/stage8_powerbi.md` Section 3.6) are used throughout this file. Key renames in use:
> - `fact_sales`: `unit_price` → `Item Price (BRL)`, `freight_value` → `Freight Cost (BRL)`
> - `fact_fx_rates`: `base_currency` → `Base Currency Code`, `quote_currency` → `Quote Currency Code`, `rate` → `Exchange Rate`
> - `fact_weather_daily`: `precipitation` → `Precipitation (mm)`, `weathercode` → `Weather Condition Code`
> - `dim_customer`: `city` → `Customer City`, `state` → `Customer State`
> - `dim_store`: `state` → `Seller State`
>
> If rebuilding the Power BI model, these renames must be applied in Power Query (Transform Data) before creating any measures. Without them every FX and weather measure will return BLANK at runtime.

---

## Organisation — Display Folders

All measures live in the `_Measures` table. Use the display folder property in Power BI to group them.

| Folder | Measures | Purpose |
|---|---|---|
| Sales | 8 | Core revenue and order KPIs |
| Time Intelligence | 5 | Period-over-period and running totals |
| FX & Multi-Currency | 4 | Exchange rates and USD conversion |
| Weather | 3 | Rainfall/weather impact on revenue |
| Operations | 3 | Delivery performance |
| Metadata | 4 | Data freshness and row counts |

> **Hide from report view:** `_BRL per USD` (helper measure — prefixed with `_`). All `Metadata` measures should be surfaced on a dedicated QA page only, not on business-facing pages.

---

## Naming conventions

- Monetary measures always carry the currency in the name if it is not BRL (e.g., `Revenue in USD`).
- BRL measures omit the currency suffix — BRL is the model's native currency.
- Helper/intermediate measures are prefixed with `_` and hidden.
- `Total Revenue` = product GMV (unit_price × quantity). Does **not** include freight. Freight is tracked separately as an operational cost. The companion `Total Sales incl. Freight` adds freight for financial completeness.
- `Order Count` = distinct orders. `Units Sold` = count of line items (= units, since quantity = 1 in this dataset).

---

## Sales folder

### Total Revenue

```dax
Total Revenue =
SUMX(
    'Sales Transactions',
    'Sales Transactions'[Item Price (BRL)] * 'Sales Transactions'[quantity]
)
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `#,##0.00` |
| Business definition | Sum of item sale prices across all transactions. Excludes freight. Equivalent to Gross Merchandise Value (GMV). |
| Expected range (full dataset) | ~BRL 13.5 M – 16 M |
| Note | `SUMX` rather than `SUM` so the formula remains correct if multi-unit lines are introduced. `quantity` is currently always 1 per the DDL. |

---

### Total Sales incl. Freight

```dax
Total Sales incl. Freight =
[Total Revenue] + [Total Freight]
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `#,##0.00` |
| Business definition | Total customer payment for both products and shipping. Use for financial reporting where the full transaction value is required. |
| Note | This is the total outlay from the customer's perspective. Use `Total Revenue` for product performance analysis; use this measure for P&L or customer spend analysis. |

---

### Order Count

```dax
Order Count =
DISTINCTCOUNT('Sales Transactions'[order_code])
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `#,##0` |
| Business definition | Number of distinct customer orders placed. One order can contain multiple line items. |
| Expected range (full dataset) | ~95,000 – 99,440 (after Silver quarantine of cancelled orders) |
| Note | Must use `DISTINCTCOUNT(order_code)`, not `COUNTROWS` — the fact grain is line item, not order. |

---

### Units Sold

```dax
Units Sold =
SUM('Sales Transactions'[quantity])
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `#,##0` |
| Business definition | Total units of product sold. Currently equals line item count because quantity = 1 per line in this dataset. `SUM(quantity)` is used rather than `COUNTROWS` so the measure remains correct if multi-unit lines are introduced. |
| Expected range (full dataset) | ~110,000 – 112,650 |

---

### Average Order Value

```dax
Average Order Value =
DIVIDE([Total Revenue], [Order Count], BLANK())
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `#,##0.00` |
| Business definition | Average product revenue per distinct order. Excludes freight. For total average transaction value including freight, use `Total Sales incl. Freight / Order Count`. |
| Expected range (full dataset) | ~BRL 130 – 160 per order |
| Note | `BLANK()` as the alternate-result for `DIVIDE` returns blank (not zero) when there are no orders in context — appropriate for KPI cards where a zero would be misleading. |

---

### Total Freight

```dax
Total Freight =
SUM('Sales Transactions'[Freight Cost (BRL)])
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `#,##0.00` |
| Business definition | Total shipping cost across all line items. Used as the numerator for freight efficiency ratios. |
| Expected range (full dataset) | ~BRL 2.2 M – 2.8 M |

---

### Freight % of Product Revenue

```dax
Freight % of Product Revenue =
DIVIDE([Total Freight], [Total Revenue], 0)
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `0.0%` |
| Business definition | Freight cost as a percentage of product GMV. Measures logistics cost burden relative to merchandise value. High values (>20%) indicate logistics pressure in a category or region. |
| Expected range (full dataset) | ~15% – 22% |
| Note | The denominator is product-only revenue. To express freight as a share of total customer spend (product + freight), use `DIVIDE([Total Freight], [Total Sales incl. Freight], 0)`. Returns 0 when revenue is zero (suitable for operational KPI cards). |

---

### Avg Freight per Order

```dax
Avg Freight per Order =
DIVIDE([Total Freight], [Order Count], BLANK())
```

| Property | Value |
|---|---|
| Display folder | Sales |
| Format string | `#,##0.00` |
| Business definition | Average shipping cost per distinct customer order. Reflects the per-order logistics cost regardless of how many items were in the order. |
| Expected range (full dataset) | ~BRL 20 – 28 per order |

---

## Time Intelligence folder

> **Prerequisite:** `Calendar` must be marked as the date table using the `date` column (DATE type) before any of these measures will work. Marking on `date_key` (INT) will cause errors.

---

### Revenue YTD

```dax
Revenue YTD =
TOTALYTD([Total Revenue], Calendar[date])
```

| Property | Value |
|---|---|
| Display folder | Time Intelligence |
| Format string | `#,##0.00` |
| Business definition | Cumulative revenue from 1 January to the selected date within the filtered year. Resets at the start of each calendar year. |
| Note | The dataset covers Sep 2016 – Oct 2018. The 2016 and 2018 YTD figures represent partial years (4 months and 10 months respectively) — label visuals accordingly to prevent stakeholders from reading partial-year YTD as a full-year figure. |

---

### Revenue MTD

```dax
Revenue MTD =
TOTALMTD([Total Revenue], Calendar[date])
```

| Property | Value |
|---|---|
| Display folder | Time Intelligence |
| Format string | `#,##0.00` |
| Business definition | Cumulative revenue from the first day of the selected month to the selected date. Resets on the 1st of each month. |

---

### Revenue MoM %

```dax
Revenue MoM % =
VAR _current = [Total Revenue]
VAR _prior =
    CALCULATE(
        [Total Revenue],
        DATEADD(Calendar[date], -1, MONTH)
    )
RETURN
    IF(
        ISBLANK(_prior),
        BLANK(),
        DIVIDE(_current - _prior, _prior, BLANK())
    )
```

| Property | Value |
|---|---|
| Display folder | Time Intelligence |
| Format string | `+0.0%;-0.0%;0.0%` |
| Business definition | Percentage change in revenue versus the same calendar month in the prior period. Returns BLANK for the first month in the dataset (September 2016) because no prior month exists. |
| Note | The `IF(ISBLANK(_prior), BLANK(), ...)` guard ensures the earliest month returns blank rather than a misleading 100% growth figure. Format string with sign prefix (`+0.0%;-0.0%`) makes positive/negative immediately visible. |

---

### Revenue YoY %

```dax
Revenue YoY % =
VAR _current = [Total Revenue]
VAR _prior =
    CALCULATE(
        [Total Revenue],
        DATEADD(Calendar[date], -1, YEAR)
    )
RETURN
    IF(
        ISBLANK(_prior),
        BLANK(),
        DIVIDE(_current - _prior, _prior, BLANK())
    )
```

| Property | Value |
|---|---|
| Display folder | Time Intelligence |
| Format string | `+0.0%;-0.0%;0.0%` |
| Business definition | Percentage revenue change versus the same period one year earlier. Returns BLANK for any period in 2016 (no prior-year data). |
| Expected behaviour | Strong positive growth expected (50%–150% YoY) for 2017 vs 2016 comparable months — Olist grew rapidly during this period. |
| Note | The dataset spans only Sep 2016 – Oct 2018, providing one complete YoY window. The formula computes `(current - prior) / prior` (a change ratio), not `current / prior` (a growth ratio). **Year-grain warning:** If a visual shows this measure at year granularity with 2017 selected, `DATEADD(-1, YEAR)` shifts to 2016, which only has 4 months of data (Sep–Dec). The resulting YoY % compares full-year 2017 against a 4-month 2016 baseline and will produce an inflated positive figure. Use this measure at month or quarter granularity only, or add a visual-level filter to exclude 2016 from YoY comparisons. |

---

### Rolling 30-Day Revenue

```dax
Rolling 30-Day Revenue =
VAR _max_date = MAX(Calendar[date])
RETURN
    CALCULATE(
        [Total Revenue],
        DATESINPERIOD(Calendar[date], _max_date, -30, DAY)
    )
```

| Property | Value |
|---|---|
| Display folder | Time Intelligence |
| Format string | `#,##0.00` |
| Business definition | Revenue over the 30 calendar days ending on the latest date in the current filter context. Useful for smoothing weekly seasonality in trend analysis. |
| Note | The window is based on the MAX date in the current slicer selection. At the very start of the dataset the window naturally truncates (fewer than 30 days of available data) — this is correct behaviour. |

---

## FX & Multi-Currency folder

> **Frankfurter API note:** The pipeline's FX extraction uses the Frankfurter (ECB) API, which always quotes with EUR as the base currency. The `Exchange Rates` table contains rows where `Base Currency Code = "EUR"`. There is no direct BRL/USD pair — it must be derived as the cross-rate: **BRL per USD = EUR/BRL ÷ EUR/USD**.

---

### Latest EUR/BRL Rate

```dax
Latest EUR/BRL Rate =
VAR _max_date =
    CALCULATE(
        MAX('Exchange Rates'[date_key]),
        'Exchange Rates'[Base Currency Code] = "EUR",
        'Exchange Rates'[Quote Currency Code] = "BRL"
    )
RETURN
    CALCULATE(
        MAX('Exchange Rates'[Exchange Rate]),
        'Exchange Rates'[date_key] = _max_date,
        'Exchange Rates'[Base Currency Code] = "EUR",
        'Exchange Rates'[Quote Currency Code] = "BRL"
    )
```

| Property | Value |
|---|---|
| Display folder | FX & Multi-Currency |
| Format string | `0.00000` |
| Business definition | Most recent EUR→BRL exchange rate available in the dataset. Reads: "1 EUR buys this many BRL." |
| Expected range | ~3.3 – 4.4 BRL per EUR (2016–2018 range) |
| Note | Uses `MAX(date_key)` within the EUR/BRL pair filter to find the latest available date, then retrieves the rate for that date. Returns BLANK if no EUR/BRL pair exists in the loaded data. **Date-context note:** There is no active relationship between `Exchange Rates` and `Calendar`. This measure always returns the latest rate within the `Exchange Rates` table's own filter context — it ignores `Calendar` date slicers. In a year-sliced matrix it will show the same rate for every year column. Use it only on header KPI cards, not in time-series visuals. |

---

### Latest EUR/USD Rate

```dax
Latest EUR/USD Rate =
VAR _max_date =
    CALCULATE(
        MAX('Exchange Rates'[date_key]),
        'Exchange Rates'[Base Currency Code] = "EUR",
        'Exchange Rates'[Quote Currency Code] = "USD"
    )
RETURN
    CALCULATE(
        MAX('Exchange Rates'[Exchange Rate]),
        'Exchange Rates'[date_key] = _max_date,
        'Exchange Rates'[Base Currency Code] = "EUR",
        'Exchange Rates'[Quote Currency Code] = "USD"
    )
```

| Property | Value |
|---|---|
| Display folder | FX & Multi-Currency |
| Format string | `0.00000` |
| Business definition | Most recent EUR→USD exchange rate available in the dataset. Reads: "1 EUR buys this many USD." |
| Expected range | ~1.04 – 1.25 USD per EUR (2016–2018 range) |
| Note | Same date-context limitation as `Latest EUR/BRL Rate` — ignores `Calendar` slicers. Use on header KPI cards only. |

---

### _BRL per USD (hidden)

```dax
_BRL per USD =
DIVIDE(
    [Latest EUR/BRL Rate],
    [Latest EUR/USD Rate],
    BLANK()
)
```

| Property | Value |
|---|---|
| Display folder | FX & Multi-Currency |
| Format string | `0.00000` |
| **Hidden** | Yes (prefix `_` convention) |
| Business definition | Derived BRL/USD cross-rate. Reads: "1 USD buys this many BRL." Computed as EUR/BRL ÷ EUR/USD because no direct BRL/USD pair is available in the ECB dataset. |
| Expected range | ~3.1 – 4.0 BRL per USD (2016–2018 range) |
| Note | This is a single-point rate using the latest available date in the `Exchange Rates` filter context — it does **not** respond to `Calendar` date slicers because there is no active relationship between `Exchange Rates` and `Calendar`. It is useful for header KPI cards showing the current rate. **`Revenue in USD` does NOT use this measure** — it recalculates the cross-rate per transaction date inside a SUMX loop for accuracy. Do not simplify `Revenue in USD` to `DIVIDE([Total Revenue], [_BRL per USD])` — that would apply a single latest-period rate to a multi-period revenue sum. |

---

### Revenue in USD

```dax
Revenue in USD =
SUMX(
    SUMMARIZE('Sales Transactions', 'Sales Transactions'[date_key]),
    VAR _tdate = 'Sales Transactions'[date_key]
    VAR _daily_rev =
        CALCULATE(
            [Total Revenue],
            'Sales Transactions'[date_key] = _tdate
        )
    VAR _eur_brl =
        CALCULATE(
            MAX('Exchange Rates'[Exchange Rate]),
            'Exchange Rates'[date_key] = _tdate,
            'Exchange Rates'[Base Currency Code] = "EUR",
            'Exchange Rates'[Quote Currency Code] = "BRL"
        )
    VAR _eur_usd =
        CALCULATE(
            MAX('Exchange Rates'[Exchange Rate]),
            'Exchange Rates'[date_key] = _tdate,
            'Exchange Rates'[Base Currency Code] = "EUR",
            'Exchange Rates'[Quote Currency Code] = "USD"
        )
    VAR _brl_per_usd = DIVIDE(_eur_brl, _eur_usd, BLANK())
    RETURN
        DIVIDE(_daily_rev, _brl_per_usd, 0)
)
```

| Property | Value |
|---|---|
| Display folder | FX & Multi-Currency |
| Format string | `#,##0.00` |
| Business definition | Total Revenue converted to USD using the actual daily BRL/USD cross-rate for each transaction date. Provides a USD-comparable revenue figure for international benchmarking. |
| Expected range (full dataset) | ~USD 3.5 M – 5 M (varies significantly by period due to BRL/USD volatility 2016–2018) |
| Note | Iterates over distinct transaction dates (~700 dates), not individual rows, for performance. Applies the EUR/BRL ÷ EUR/USD cross-rate specific to each date. Returns 0 when no FX data exists for a date (forward-fill in the pipeline should prevent gaps). **Report disclaimer required:** USD amounts reflect spot rates on each transaction date; BRL/USD swung ~30% across the dataset period. Trend comparisons in USD blend business performance with currency effects. |

---

## Weather folder

> **Join method:** `Sales Transactions` does not have a direct relationship to `Daily Weather Conditions`. The join is enforced per-row: for each sales transaction, the measure looks up the weather record matching the transaction's `date_key` AND the customer's `city` + `state` (retrieved via `RELATED(Customers[...])`). This requires `customer_key` to be non-null for a match to succeed. Transactions with no customer match (null customer_key) will always contribute 0 to weather-filtered measures.
>
> **Precipitation threshold:** 10 mm (moderate or heavy rain, likely to influence outdoor activity). The meteorological minimum of 1 mm is too low for behavioural analysis in a tropical climate — it would classify most Brazilian days as rainy.

---

### Revenue on Rainy Days

```dax
Revenue on Rainy Days =
SUMX(
    'Sales Transactions',
    VAR _date    = 'Sales Transactions'[date_key]
    VAR _city    = RELATED(Customers[Customer City])
    VAR _state   = RELATED(Customers[Customer State])
    VAR _precip  =
        CALCULATE(
            MAX('Daily Weather Conditions'[Precipitation (mm)]),
            'Daily Weather Conditions'[date_key] = _date,
            'Daily Weather Conditions'[city]     = _city,
            'Daily Weather Conditions'[state]    = _state
        )
    RETURN
        IF(
            NOT ISBLANK(_precip) && _precip > 10,
            'Sales Transactions'[Item Price (BRL)] * 'Sales Transactions'[quantity],
            0
        )
)
```

| Property | Value |
|---|---|
| Display folder | Weather |
| Format string | `#,##0.00` |
| Business definition | Revenue from transactions where the customer's city and state received more than 10 mm of precipitation on the order date. Used to analyse whether heavy rain correlates with increased online purchasing. |
| Note | The 10 mm threshold represents moderate or heavy rainfall that is likely to affect outdoor behaviour. The geographic match (customer city + state + date must all match a weather record) means transactions with no customer match are excluded. Performance: iterates all rows in the current filter context — acceptable for a 112K-row Import model. |

---

### Revenue on Clear Days

```dax
Revenue on Clear Days =
SUMX(
    'Sales Transactions',
    VAR _date    = 'Sales Transactions'[date_key]
    VAR _city    = RELATED(Customers[Customer City])
    VAR _state   = RELATED(Customers[Customer State])
    VAR _wcode   =
        CALCULATE(
            MAX('Daily Weather Conditions'[Weather Condition Code]),
            'Daily Weather Conditions'[date_key] = _date,
            'Daily Weather Conditions'[city]     = _city,
            'Daily Weather Conditions'[state]    = _state
        )
    RETURN
        IF(
            NOT ISBLANK(_wcode) && _wcode IN {0, 1, 2},
            'Sales Transactions'[Item Price (BRL)] * 'Sales Transactions'[quantity],
            0
        )
)
```

| Property | Value |
|---|---|
| Display folder | Weather |
| Format string | `#,##0.00` |
| Business definition | Revenue from transactions where the customer's location had clear or mainly clear skies on the order date (WMO codes 0 = clear sky, 1 = mainly clear, 2 = partly cloudy). |
| Note | WMO codes 0–2 represent the three clear-sky categories in the WMO 4677 standard. Transactions where the weather code is missing (no weather record for that city/date) contribute 0. `Revenue on Rainy Days + Revenue on Clear Days` will not sum to `Total Revenue` — some days have intermediate conditions (overcast, fog, drizzle) that belong to neither category. |

---

### Rainy vs Clear Revenue Ratio

```dax
Rainy vs Clear Revenue Ratio =
DIVIDE(
    [Revenue on Rainy Days],
    [Revenue on Clear Days],
    BLANK()
)
```

| Property | Value |
|---|---|
| Display folder | Weather |
| Format string | `0.00` |
| Business definition | Ratio of rainy-day revenue to clear-day revenue. A value greater than 1 indicates more revenue is generated on rainy days than clear days in absolute terms (which also reflects the relative frequency of each weather type). For a proportion-adjusted view, compare this ratio against the proportion of rainy vs clear days in the same period. |
| Note | Returns BLANK when clear-day revenue is zero (e.g., a date filter with no clear-day records). The ratio reflects raw revenue totals, not normalised per-day rates. |

---

## Operations folder

---

### Avg Delivery Days

```dax
Avg Delivery Days =
AVERAGEX(
    FILTER(
        'Sales Transactions',
        NOT ISBLANK('Sales Transactions'[delivery_days_actual])
    ),
    'Sales Transactions'[delivery_days_actual]
)
```

| Property | Value |
|---|---|
| Display folder | Operations |
| Format string | `0.0` |
| Business definition | Average number of calendar days between order placement and delivery, for all delivered orders. Undelivered orders (where delivery_days_actual is null) are excluded. |
| Expected range (full dataset) | ~10 – 14 days |
| Note | DAX `AVERAGE` ignores BLANK natively, but the explicit `FILTER(NOT ISBLANK(...))` makes the exclusion visible to readers of the measure code. **Grain note:** Delivery columns (`delivery_days_actual`, `delivery_days_estimated`) are at the order level but the fact table grain is the line item. Multi-item orders contribute one row per item, all with identical delivery values, which over-weights those orders proportionally to their item count. For the Olist dataset where the vast majority of orders are single-item, the practical impact is negligible. For strictly order-level delivery metrics, group by `order_code` first using `SUMMARIZE`. |

---

### Avg Delivery Delay (Days)

```dax
Avg Delivery Delay (Days) =
AVERAGEX(
    FILTER(
        'Sales Transactions',
        NOT ISBLANK('Sales Transactions'[delivery_days_actual])
            && NOT ISBLANK('Sales Transactions'[delivery_days_estimated])
    ),
    'Sales Transactions'[delivery_days_actual]
        - 'Sales Transactions'[delivery_days_estimated]
)
```

| Property | Value |
|---|---|
| Display folder | Operations |
| Format string | `+0.0;-0.0;0.0` |
| Business definition | Average gap between actual and estimated delivery time, in days. **Positive = late** (delivered after the promised date). **Negative = early** (delivered ahead of promise). Only rows where both actual and estimated delivery are recorded are included. |
| Expected range (full dataset) | ~+1 to +5 days (mild late bias is typical for Olist data) |
| Note | The sign convention follows the standard operations definition: positive variance = cost overrun in time. The format string with sign prefix makes this immediately visible on KPI cards. Previously named `Delivery Variance` — renamed to avoid confusion with statistical variance. Same item-grain note as `Avg Delivery Days` — multi-item orders are over-weighted. |

---

### Late Delivery Rate

```dax
Late Delivery Rate =
VAR _deliveries =
    COUNTROWS(
        FILTER(
            'Sales Transactions',
            NOT ISBLANK('Sales Transactions'[delivery_days_actual])
                && NOT ISBLANK('Sales Transactions'[delivery_days_estimated])
        )
    )
VAR _late =
    COUNTROWS(
        FILTER(
            'Sales Transactions',
            NOT ISBLANK('Sales Transactions'[delivery_days_actual])
                && NOT ISBLANK('Sales Transactions'[delivery_days_estimated])
                && 'Sales Transactions'[delivery_days_actual]
                    > 'Sales Transactions'[delivery_days_estimated]
        )
    )
RETURN
    DIVIDE(_late, _deliveries, BLANK())
```

| Property | Value |
|---|---|
| Display folder | Operations |
| Format string | `0.0%` |
| Business definition | Percentage of delivered orders that arrived after the estimated delivery date. The primary on-time delivery KPI. The inverse (On-Time Delivery Rate) = 1 − Late Delivery Rate. |
| Note | Both `delivery_days_actual` and `delivery_days_estimated` must be non-null for a row to be included in either count. Returns BLANK when no delivered-and-estimated records exist in context. |

---

## Metadata folder

> **Usage:** These measures are for data validation and pipeline monitoring. Surface them on a dedicated "Data Quality" page, not on business-facing report pages.

---

### Data Freshness

```dax
Data Freshness =
FORMAT(
    CALCULATE(
        MAX('Sales Transactions'[_loaded_at]),
        ALL('Sales Transactions')
    ),
    "YYYY-MM-DD HH:mm UTC"
)
```

| Property | Value |
|---|---|
| Display folder | Metadata |
| Format string | Text (formatted inside the DAX) |
| Business definition | Timestamp of the most recent ETL pipeline load for the Sales Transactions table. Reflects when the Power BI dataset was last refreshed with new data from the PostgreSQL warehouse. |
| Note | `ALL('Sales Transactions')` removes any active date/category slicers so the freshness card always shows the true latest load timestamp regardless of report page filters. The `_loaded_at` column is set once per ETL run for all rows in a batch, so `MAX(_loaded_at)` reliably represents the pipeline's last execution. Shows UTC time; inform stakeholders that BRT = UTC − 3. |

---

### Sales Row Count

```dax
Sales Row Count =
COUNTROWS('Sales Transactions')
```

| Property | Value |
|---|---|
| Display folder | Metadata |
| Format string | `#,##0` |
| Business definition | Total number of line item rows in the Sales Transactions fact table under the current filter context. Used to confirm data load completeness. |
| Expected range (full dataset, no filters) | ~110,000 – 112,650 |

---

### Weather Row Count

```dax
Weather Row Count =
COUNTROWS('Daily Weather Conditions')
```

| Property | Value |
|---|---|
| Display folder | Metadata |
| Format string | `#,##0` |
| Business definition | Total number of rows in the Daily Weather Conditions fact table under the current filter context. |
| Expected range (full dataset) | ~15,000 – 25,000 (depends on city count and date range covered by the Open-Meteo extraction) |

---

### FX Row Count

```dax
FX Row Count =
COUNTROWS('Exchange Rates')
```

| Property | Value |
|---|---|
| Display folder | Metadata |
| Format string | `#,##0` |
| Business definition | Total number of rows in the Exchange Rates fact table under the current filter context. |
| Expected range (full dataset) | ~760 × (number of currency pairs loaded). For two pairs (EUR/BRL + EUR/USD) with daily forward-fill across ~760 days: ~1,520 rows. Verify against actual loaded data. |

---

## Quick-reference table

| # | Measure name | Folder | Format | Hidden |
|---|---|---|---|---|
| 1 | Total Revenue | Sales | `#,##0.00` | No |
| 2 | Total Sales incl. Freight | Sales | `#,##0.00` | No |
| 3 | Order Count | Sales | `#,##0` | No |
| 4 | Units Sold | Sales | `#,##0` | No |
| 5 | Average Order Value | Sales | `#,##0.00` | No |
| 6 | Total Freight | Sales | `#,##0.00` | No |
| 7 | Freight % of Product Revenue | Sales | `0.0%` | No |
| 8 | Avg Freight per Order | Sales | `#,##0.00` | No |
| 9 | Revenue YTD | Time Intelligence | `#,##0.00` | No |
| 10 | Revenue MTD | Time Intelligence | `#,##0.00` | No |
| 11 | Revenue MoM % | Time Intelligence | `+0.0%;-0.0%;0.0%` | No |
| 12 | Revenue YoY % | Time Intelligence | `+0.0%;-0.0%;0.0%` | No |
| 13 | Rolling 30-Day Revenue | Time Intelligence | `#,##0.00` | No |
| 14 | Latest EUR/BRL Rate | FX & Multi-Currency | `0.00000` | No |
| 15 | Latest EUR/USD Rate | FX & Multi-Currency | `0.00000` | No |
| 16 | _BRL per USD | FX & Multi-Currency | `0.00000` | **Yes** |
| 17 | Revenue in USD | FX & Multi-Currency | `#,##0.00` | No |
| 18 | Revenue on Rainy Days | Weather | `#,##0.00` | No |
| 19 | Revenue on Clear Days | Weather | `#,##0.00` | No |
| 20 | Rainy vs Clear Revenue Ratio | Weather | `0.00` | No |
| 21 | Avg Delivery Days | Operations | `0.0` | No |
| 22 | Avg Delivery Delay (Days) | Operations | `+0.0;-0.0;0.0` | No |
| 23 | Late Delivery Rate | Operations | `0.0%` | No |
| 24 | Data Freshness | Metadata | Text | No |
| 25 | Sales Row Count | Metadata | `#,##0` | No |
| 26 | Weather Row Count | Metadata | `#,##0` | No |
| 27 | FX Row Count | Metadata | `#,##0` | No |

**Total: 27 measures (1 hidden helper)**

---

## Known limitations and deferred items

| Item | Status | Notes |
|---|---|---|
| `review_score` column | Not in Gold schema | The `fact_sales` DDL has no review_score column. Review data requires a separate ETL pass to join `olist_order_reviews_dataset.csv` and load into `fact_sales` or a new `fact_reviews` table. Average Review Score and 5-Star Review Rate are deferred until the reviews ETL is built. |
| Cancellation Rate | Not computable | Cancelled orders are quarantined at the Silver layer and excluded from `fact_sales`. Cancellation rate reporting requires either a separate cancelled-orders fact table or a Bronze-level count measure. |
| `is_holiday` column | Not in Calendar | The `dim_date` DDL has no is_holiday column. Holiday-adjusted time intelligence (e.g., revenue excluding holidays) is not available without adding a holiday calendar. |
| Weather: seller-state join | Not implemented | Weather measures use customer city/state for the location match. A parallel `Revenue on Rainy Days (Seller)` measure using `Sellers[Seller State]` could be added if seller-location weather correlation is required. |
| Revenue in Constant USD | Not implemented | A `Revenue in Constant USD` measure (applying a fixed reference-date rate to all periods) would support FX-neutral trend analysis. Add if stakeholders need to separate business performance from currency effects. |
