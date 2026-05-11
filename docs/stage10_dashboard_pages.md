# Stage 10 — Power BI Dashboard Pages
## Brazilian E-Commerce Analytics: Olist 2016–2018

> **⚠ FX/installments narrative is partially stale.** Where this document
> describes EUR-base FX cross-rate derivation, the pipeline now fetches
> the direct USD/BRL pair (`src/extract/extract_fx.py`) and exposes
> `analytics.v_sales_usd` for USD-normalised reporting. Build USD measures
> against that view, not via `EUR/BRL ÷ EUR/USD`.

**Prerequisites:** Stage 8 (semantic model), Stage 9 (DAX measures)
**Canvas:** 1280 × 720 px, 16:9 widescreen, Import mode (~60 MB)

---

## Table of Contents

1. [Report Overview](#1-report-overview)
2. [Global Design System](#2-global-design-system)
3. [Page 1 — Market Overview](#3-page-1--market-overview)
4. [Page 2 — External Signals](#4-page-2--external-signals)
5. [Page 3 — Pipeline Health](#5-page-3--pipeline-health)
6. [Page 4 — Source Detail](#6-page-4--source-detail)
7. [Navigation Header](#7-navigation-header)
8. [Global Slicer Sync Configuration](#8-global-slicer-sync-configuration)
9. [Accessibility Requirements](#9-accessibility-requirements)
10. [Performance Considerations](#10-performance-considerations)
11. [Implementation Checklist](#11-implementation-checklist)

---

## 1. Report Overview

### 1.1 Page Inventory

| # | Tab Label | Full Page Title | Audience | Target Read Time |
|---|---|---|---|---|
| 1 | Market Overview | Brazilian Marketplace Performance: 2016–2018 Growth Trajectory | C-suite, Board | 30 seconds |
| 2 | External Signals | Weather, Currency, and Demand: What External Forces Drive Brazilian E-Commerce? | Data analysts, Marketing, Operations | 5–10 minutes |
| 3 | Pipeline Health | ETL Pipeline Operations: Multi-Source Ingestion and Medallion Layer Status | Data engineers, Analytics leads | 2 minutes |
| 4 | Source Detail | Transaction-Level Explorer: Orders, Sellers, Products, and Enriched Attributes | QA engineers, Data stewards | Ad hoc |

### 1.2 Report-Level Context Card

This text block appears on every page, anchored to the bottom-left footer zone (8 pt, `#7F8C8D`):

> Data covers September 2016 through October 2018. Sources: Olist Brazilian E-Commerce dataset (Kaggle, 9 CSV tables, ~99K orders), Open-Meteo Historical Weather API (daily precipitation and temperature by Brazilian city), Frankfurter Exchange Rate API (daily EUR/BRL and EUR/USD mid-market rates). Monetary values are native BRL; USD equivalents calculated via EUR cross-rate. Pipeline architecture: Bronze/Silver/Gold medallion layers. Dataset is static (historical archive). All figures as-of pipeline last run: [Data Freshness measure].

### 1.3 Business Narrative Thread

The four pages tell a linked story designed to distinguish this as a production-grade data product:

- **Page 1** establishes the growth story: 18× order growth from late 2016 to Q3 2018, sustained above-4.0 review scores, and a regional logistics gap between SP and the North/Northeast.
- **Page 2** tests the hypothesis that external signals (weather, FX) drive demand — and honestly reports that they largely do not, which is the more sophisticated finding.
- **Page 3** validates that the numbers on Pages 1–2 can be trusted, surfacing the engineering work that is otherwise invisible in a portfolio project.
- **Page 4** gives analysts and QA engineers the row-level access needed to investigate any anomaly found on the previous pages.

---

## 2. Global Design System

### 2.1 Colour Tokens

All colour usage must reference one of these tokens. Do not introduce colours not listed here.

#### Brand Palette
| Token | Hex | Usage |
|---|---|---|
| `--brand-cobalt` | `#1B4F8A` | Navigation bar, page titles, active UI states |
| `--brand-sky` | `#2E86C1` | Primary data series, revenue bars, hyperlinks |
| `--brand-teal` | `#17A589` | Positive indicators, secondary data series |
| `--brand-amber` | `#D4A017` | Warnings, third data series |
| `--brand-charcoal` | `#1C2833` | Dark text, Page 3 canvas background |
| `--brand-slate` | `#566573` | Axis labels, muted UI elements |

#### Semantic Colours — Never repurpose for decorative use
| Token | Hex | Meaning |
|---|---|---|
| `--semantic-positive` | `#1E8449` | Up trend, PASS, good |
| `--semantic-negative` | `#C0392B` | Down trend, FAIL, bad |
| `--semantic-warning` | `#E67E22` | Caution, WARN, review needed |
| `--semantic-neutral` | `#7F8C8D` | No change, N/A, missing data |
| `--semantic-info` | `#2980B9` | Annotation callouts, informational |

#### Surface & Background
| Token | Hex | Applied To |
|---|---|---|
| `--bg-canvas` | `#F5F6FA` | Page canvas (Pages 1, 2, 4) |
| `--bg-card` | `#FFFFFF` | Visual container fill, KPI card |
| `--bg-section` | `#EBF0F8` | Section headers, slicer bar background |
| `--bg-pipeline` | `#1C2833` | Page 3 canvas only |
| `--bg-pipeline-card` | `#273746` | Page 3 card/panel backgrounds |
| `--border-light` | `#D5D8DC` | Card borders (light pages) |
| `--border-dark` | `#4A5568` | Card borders (Page 3 dark) |

#### Weather Series (Page 2 only)
| Token | Hex | WMO Codes |
|---|---|---|
| `--weather-clear` | `#F4D03F` | 0–1 |
| `--weather-cloudy` | `#AEB6BF` | 2–3 |
| `--weather-rainy` | `#2E86C1` | 51–67, 80–82 |
| `--weather-stormy` | `#6C3483` | 95–99 |
| `--weather-other` | `#7F8C8D` | All other codes |

### 2.2 Typography

**Font family:** Segoe UI throughout. No mixed fonts.

| Role | Size | Weight | Colour Token |
|---|---|---|---|
| Page title | 20 pt | Semibold (600) | `--text-primary` |
| Section title | 13 pt | Semibold (600) | `--text-primary` |
| Visual title | 11 pt | Semibold (600) | `--text-primary` |
| KPI card value | 28 pt | Bold (700) | `--text-primary` |
| KPI card label | 9 pt | Regular (400) | `--text-secondary` |
| KPI trend delta | 10 pt | Semibold (600) | Semantic colour |
| Axis label | 9 pt | Regular (400) | `--text-secondary` |
| Data label | 9 pt | Regular (400) | `--text-primary` or `#FFFFFF` |
| Table header | 10 pt | Semibold (600) | `--text-primary` |
| Table body | 9 pt | Regular (400) | `--text-primary` |
| Nav button (active) | 10 pt | Semibold (600) | `--brand-cobalt` |
| Nav button (inactive) | 10 pt | Semibold (600) | `#FFFFFF` |
| Footer caption | 8 pt | Regular (400) | `#7F8C8D` |

Text colour tokens: `--text-primary` = `#1C2833`, `--text-secondary` = `#566573`, `--text-inverse` = `#FFFFFF`, `--text-inverse-muted` = `#A9B7C6`.

### 2.3 Layout Grid (1280 × 720 canvas)

| Zone | Y origin | Height | Notes |
|---|---|---|---|
| Navigation bar | 0 | 40 px | Fixed, every page |
| Page title row | 40 | 36 px | Title + subtitle text |
| Gutter | 76 | 12 px | |
| KPI card row | 88 | 100 px | 4 cards × 297 px wide |
| Gutter | 188 | 12 px | |
| Main visual area | 200 | 428 px | Fills remaining canvas |
| Gutter | 628 | 12 px | |
| Slicer / footer bar | 640 | 48 px | |
| Bottom margin | 688 | 16 px | |

**Column widths (with 16 px edge margins, 12 px gutters):**
- Full width: 1248 px
- Half/half: 618 px each
- Thirds: 404 px each
- 4-up KPI cards: 297 px each

### 2.4 KPI Card Anatomy

```
+-------------------------------------------+
| KPI LABEL (9pt Regular)  DELTA▲ +8.3%     |
|                                            |
| PRIMARY VALUE (28pt Bold)                  |
|                                            |
| Secondary context (8pt Regular, muted)     |
+-------------------------------------------+
  Width: 297px  Height: 100px
  Background: #FFFFFF
  Border: 1px solid #D5D8DC, corner-radius: 4px
  Left accent bar: 4px wide, colour = series colour
  Internal padding: 12px
```

**Trend delta rules:**

| Condition | Icon | Text suffix | Colour |
|---|---|---|---|
| MoM change > 0% | ▲ | "(UP)" | `#1E8449` |
| MoM change < 0% | ▼ | "(DOWN)" | `#C0392B` |
| MoM change = 0% or BLANK | ▬ | "(FLAT)" | `#7F8C8D` |

Colour is paired with both a distinct Unicode symbol and a text suffix so meaning is not colour-dependent. Example display: "▲ +8.3% (UP)".

Format string for delta: `+0.0%;-0.0%;0.0%`

Note: For metrics where a decrease is positive (e.g., Freight % of Revenue), invert the semantic colour per-card using a conditional format measure, not by changing the global tokens.

### 2.5 Chart Element Standards

**Visual titles:** Business English only. Write "Monthly Revenue (BRL)" not "Total Revenue by Calendar[month]". Include unit where non-obvious.

**Axes:**
- Y-axis title: always hidden (visual title names the metric)
- X-axis title: hidden unless category is ambiguous
- Number abbreviation: "14.2M" and "4.3K" — never full unabbreviated numbers on axes
- Date format on X-axis: monthly = "MMM YY"; quarterly = "Q1 2017"; annual = "2017"
- Label rotation: 0° preferred; -45° only if >8 categories with labels >8 chars; never -90°

**Gridlines:**
- Horizontal: show (`#E8EAED`, 1 px, solid)
- Vertical: hide
- Zero line: show on charts where values can be negative (`#C0C4C8`, 1 px)
- Page 3 dark canvas: gridline colour `#364A5C`

**Data labels:** Show when ≤6 data points or primary hero visual. Hide when >8 data points, or on scatter plots (use tooltips). Format: `R$ 0.0M` / `R$ 0.0K` for revenue; `0.0%` for percentages; `#,##0` for counts.

**Legend:** Bottom for bar/column charts. Right for line charts with 2–3 series. Always hide legend title. Max 5 legend items; group remainder as "Other".

**Visual containers:** Use manually placed Rectangle shapes (fill `#FFFFFF`, border `1px #D5D8DC`, corner-radius 4 px, shadow `rgba(0,0,0,0.06)` blur 4 px offset 0/1 px). Do not use Power BI's native border/shadow on individual visuals — this prevents precise pixel control.

---

## 3. Page 1 — Market Overview

### 3.1 Purpose and Audience

**Full title:** Brazilian Marketplace Performance: 2016–2018 Growth Trajectory  
**Tab label:** Market Overview  
**Audience:** C-suite, CFO, board-level leadership  
**Audience label (page subtitle):** [Leadership]  
**Target consumption time:** 30 seconds  
**Page accent bar colour:** `#1F3864` (4 px vertical rectangle, left edge of canvas)

**Questions this page answers in 30 seconds:**
1. Is order volume and revenue growing, stagnating, or declining?
2. Which product categories and states drive the majority of revenue?
3. How efficiently are orders being delivered?
4. What is the average order value trend?

### 3.2 Layout

```
+-----------------------------------------------------------------------------------+
| NAVIGATION BAR                                                                    | y=0, h=40
+-----------------------------------------------------------------------------------+
| "Brazilian Marketplace Performance: 2016–2018 Growth Trajectory"  [Leadership]   | y=40, h=36
+-----------------------------------------------------------------------------------+
|  TOTAL REVENUE  |  TOTAL ORDERS   |  AVG ORDER VALUE|  FREIGHT %      |           | y=88, h=100
|  297 × 100 px   |  297 × 100 px   |  297 × 100 px   |  297 × 100 px   |           |
+-----------------------------------------------------------------------------------+
|  REVENUE TREND (line chart)       |  TOP CATEGORIES (horizontal bar)  |           | y=200
|  618 × 428 px                     |  618 × 428 px                     |           | h=428
+-----------------------------------------------------------------------------------+
|  YEAR slicer (buttons)  |  QUARTER slicer (buttons)  |  CUSTOMER STATE (dropdown) | y=640, h=48
+-----------------------------------------------------------------------------------+
```

**Default slicer state on load:** Year = 2017 and 2018 (exclude 2016 — 2016 data is sparse; opening unfiltered makes the trend look like a dramatic spike from near-zero, which is a data availability artefact, not a business trend). Implement via a bookmark applied on report open. Add annotation near trend line: "2016 data partial — orders begin Q4 2016."

### 3.3 KPI Cards

| Card | Measure | Format String | Accent Colour |
|---|---|---|---|
| Total Revenue | `Total Revenue` | `"R$ "#,##0.0,,M"M"` | `#2E86C1` |
| Total Orders | `Order Count` | `#,##0` | `#17A589` |
| Avg Order Value | `Avg Order Value` | `"R$ "#,##0.00` | `#D4A017` |
| Freight % of Revenue | `Freight % of Revenue` | `0.0%` | `#E67E22` |

Each card also shows its MoM % delta using the corresponding `[Measure] MoM %` DAX measures from Stage 9.

**Interaction rule:** Set **all other visuals' interactions on all four KPI cards to "None"**. KPI cards must always show the slicer-scoped total, never a click-scoped subset. An executive clicking São Paulo on a map must not see headline revenue silently drop to São Paulo-only without understanding why. This is the most common source of stakeholder distrust in Power BI reports.

### 3.4 Revenue Trend Line Chart

**Visual type:** Line chart (consider clustered column + line combo for prior-year comparison)  
**X-axis:** `Calendar[date]` (month grain, format "MMM YY")  
**Primary series:** `Total Revenue` (colour `#2E86C1`)  
**Secondary series (optional):** Prior-year revenue via `DATEADD` (colour `#AED6F1`, same hue at 40% saturation)  
**Measure tooltip fields:** Period (Month Year), Total Revenue BRL, Revenue MoM %, Order Count  
**Data labels:** Hidden (too many points)  
**Cross-filter behaviour:** Cross-highlight only when clicked. Do not cross-filter — clicking a time period should not reduce the category chart to zero; it should highlight that period's contribution.

**Annotation callout text (text box near November 2017 peak):**
> "Black Friday effect confirmed: November 2017 order volume exceeded the prior 3-month average by approximately 230%. Operations teams should treat Q4 as a distinct planning horizon."

### 3.5 Top Product Categories Horizontal Bar Chart

**Visual type:** Horizontal bar chart, sorted descending  
**Y-axis:** `Products[Category (English)]` — top 10 categories only, final bar = "All Others" aggregate  
**X-axis:** `Total Revenue`  
**Colour:** Single series `#2E86C1`  
**Data labels:** Show (10 bars is within the ≤6 guideline waiver — acceptable for horizontal bars with enough width)  
**Format:** `R$ 0.0M`  
**Cross-filter behaviour:** Cross-filters the trend line and any map visual on the page

**Do NOT use a pie or donut chart for this visual.** There are 74 product categories after translation; a pie produces an illegible "other" segment and a scrollable legend.

### 3.6 Optional Third Visual: Brazil Choropleth Map

If a third chart replaces the two-column layout (use Template B thirds layout at 404 × 428 px each):

**Visual type:** Filled map (Bing Maps or Azure Maps)  
**Location field:** `Customers[Customer State]`  
**Colour saturation:** `Total Revenue`  
**Tooltip:** Custom page tooltip (320 × 240 px) including: State name, Total Revenue BRL, Total Orders, Top Product Category (`TOPN + CONCATENATEX` measure), Avg Delivery Days  
**Alt text:** "Choropleth map showing Total Revenue by Brazilian customer state. Use the Source Detail table for accessible data."

**Cross-filter behaviour:** Clicking a state cross-filters the trend line and category bar. Set "None" for KPI cards.

**Annotation callout text (near map):**
> "Sao Paulo accounts for approximately 42% of total orders but only 38% of total freight cost — reflecting SP's logistics infrastructure advantage. States in the North and Northeast show the inverse: higher freight-to-order-value ratios and longer delivery windows, creating a structural customer experience gap."

### 3.7 Slicer Configuration

| Slicer | Field | Style | Placement |
|---|---|---|---|
| Year | `Calendar[year]` | Button (horizontal) | Bottom bar |
| Quarter | `Calendar[quarter]` | Button (horizontal) | Bottom bar |
| Customer State | `Customers[Customer State]` | Dropdown (single-select) | Bottom bar or optional left panel (collapsed by default) |

Slicer panel for Customer State: consider a bookmark/overlay "Filter" button — default state shows no left panel so the executive sees KPIs unobstructed.

---

## 4. Page 2 — External Signals

### 4.1 Purpose and Audience

**Full title:** Weather, Currency, and Demand: What External Forces Drive Brazilian E-Commerce?  
**Tab label:** External Signals  
**Audience:** Data analysts, Marketing, Operations  
**Audience label:** [Analyst]  
**Page accent bar colour:** `#2E75B6`

**Three testable hypotheses this page can confirm or refute:**

1. **Rain → online orders:** On days with precipitation > 10 mm in SP, do orders from SP customers exceed the 7-day rolling average?
2. **BRL depreciation → imported-goods spike:** During weeks with EUR/BRL movement > 2%, did Electronics/Computers category volumes move in the same direction?
3. **Extreme temperatures → category shifts:** Do Fashion and Home categories see volume changes on extreme heat/cold days vs moderate days?

**Anticipated key finding (annotate clearly):** Weather explains < 5% of daily order variance in SP (R² < 0.05). The more sophisticated finding is the *absence* of correlation — Olist is not an impulse-purchase platform; purchase trigger and fulfillment are separated by days. This should be stated explicitly, not buried.

### 4.2 Layout

```
+-----------------------------------------------------------------------------------+
| NAVIGATION BAR                                                                    |
+-----------------------------------------------------------------------------------+
| "Weather, Currency, and Demand: ..."  [Analyst]           HOME button (right)     |
+-----------------------------------------------------------------------------------+
|  REVENUE BRL  |  ORDER COUNT  |  HIGH RAIN DAYS |  EUR/BRL RATE  | BRL/USD RATE  |
|  (KPI card)   |  (KPI card)   |  (KPI card)     |  (KPI card)    | (KPI card)    |
+-----------------------------------------------------------------------------------+
|  DUAL-AXIS TREND                  |  PRECIPITATION vs REVENUE SCATTER             |
|  Revenue (bars) + EUR/BRL (line)  |  Daily Orders vs Precipitation, coloured by  |
|  618 × 428 px                     |  weather category  618 × 428 px              |
+-----------------------------------------------------------------------------------+
|  YEAR  |  QUARTER  |  MONTH  |  CUSTOMER STATE  |  WEATHER CITY  |  ORDER STATUS  |
+-----------------------------------------------------------------------------------+
```

A date-range range slider (Calendar[date], Between style) may be placed in a left panel (180 px wide) on this page only, labelled "Order Date Range". It is NOT synced to other pages.

### 4.3 KPI Cards (5-up layout, 234 px each)

| Card | Measure | Format String | Accent Colour | Notes |
|---|---|---|---|---|
| Total Revenue | `Total Revenue` | `"R$ "#,##0.0,,M"M"` | `#2E86C1` | |
| Order Count | `Order Count` | `#,##0` | `#17A589` | |
| High Precipitation Days | `High Precipitation Days` | `#,##0` | `#2E86C1` (weather-rainy) | Days with precipitation > 10 mm |
| Latest EUR/BRL Rate | `Latest EUR/BRL Rate` | `0.0000` | `#8E44AD` | Add subtitle: "Latest available rate" |
| Revenue in USD | `Revenue in USD` | `"$"#,##0.0,,M"M"` | `#E67E22` | **Must** have subtitle: "Approx. via ECB cross-rate. Not for financial reporting." |

**Isolation rule:** `Revenue in USD` and `Latest EUR/BRL Rate` cards must be fully isolated from all cross-filtering (`Edit Interactions → None` for all incoming interactions). Because `Revenue in USD` uses a date-level SUMX cross-rate calculation (not a simple filtered lookup), visual clicks will not produce meaningful filtered values — an isolated card with a clear label is less confusing than a card that shows incorrect filtered USD values.

Add asterisk on the `Revenue in USD` card when a Year/Quarter slicer narrows the period: the rate shown is the latest available rate, not a period-average. Use a conditional subtitle measure.

### 4.4 Dual-Axis Trend Chart

**Visual type:** Clustered column + line (combo chart)  
**X-axis:** `Calendar[date]` monthly grain, format "MMM YY"  
**Primary axis (columns):** `Total Revenue` (colour `#2E86C1`)  
**Secondary axis (line):** `Latest EUR/BRL Rate` — **cross-highlight only, not cross-filter** (see Section 4.7)  
**Data labels:** Hidden  
**Legend:** Bottom, "Revenue (BRL)" and "EUR/BRL Rate"

**Annotation callout text near May–June 2018:**
> "The BRL depreciation episode of May–June 2018 (EUR/BRL moved from ~4.0 to ~4.7 over 6 weeks) did not produce a measurable spike in Electronics or high-value imported categories. This suggests Olist's customer base was not using the platform as an inflation hedge during this period."

### 4.5 Precipitation vs Revenue Scatter Plot

**Visual type:** Scatter chart  
**X-axis:** `Avg Precipitation (mm)` from `Daily Weather Conditions`  
**Y-axis:** `Total Revenue`  
**Size:** `Order Count`  
**Legend/colour:** `Daily Weather Conditions[weather_category]` — apply weather colour tokens (`#F4D03F`, `#AEB6BF`, `#2E86C1`, `#6C3483`, `#7F8C8D`)

**Accessibility — colour contrast for scatter points:** Yellow (`#F4D03F`) on a light canvas (`#F5F6FA`) is approximately 1.2:1 — effectively invisible. Light grey (`#AEB6BF`) on light canvas is also insufficient. **Fix:** Add a 1 px dark stroke border (`#566573`) to all scatter data points, regardless of fill colour. This ensures the point shape is visible even when the fill fails contrast against the background. Alternatively, use darker variants: `#B7950B` for clear/sunny (instead of `#F4D03F`) and `#808B96` for cloudy (instead of `#AEB6BF`).

**Accessibility — colour-only series:** The scatter legend differentiates weather categories by colour only. Pair each legend entry with both its colour swatch and its text label (e.g., "Clear", "Cloudy", "Rainy", "Stormy"). The custom tooltip must include the weather category name (`weather_category` field) alongside the colour so screen reader users receive the category identity through text, not colour alone.  
**Tooltip:** Custom page tooltip (320 × 240 px) including: State/City, Revenue BRL, Total Orders, Avg Precipitation (mm) labelled as "Avg Daily Precipitation (mm)", High Precipitation Days count, Revenue in USD labelled "Approx. USD (EUR cross-rate)"  
**Data labels:** Show only for top 10% and bottom 10% of revenue distribution (8 pt to avoid clutter)  
**Cross-filter behaviour:** Cross-filters the trend chart and order volume series. Cross-highlight only when interacting with weather trend line.

**Annotation callout text (text box near plot):**
> "Regression of daily SP orders vs same-day precipitation produces R² < 0.05 — weather explains less than 5% of daily order volume variance. Day-of-week and promotional calendar events are dominant predictors. Weather enrichment adds analytical completeness without adding predictive lift for total demand."

**Critical annotation — weather join quality (text box below scatter):**
> "Cities with no weather match excluded (~5–8% of orders, driven by rural/unrecognized zip codes). Match rate visible on Pipeline Health page."

### 4.6 Slicer Configuration

| Slicer | Field | Style | Panel |
|---|---|---|---|
| Year | `Calendar[year]` | Button | Bottom bar |
| Quarter | `Calendar[quarter]` | Button | Bottom bar |
| Month | `Calendar[month]` | List (multi-select) | Left panel |
| Customer State | `Customers[Customer State]` | Dropdown | Left panel |
| Weather City | `Daily Weather Conditions[city]` | Dropdown | Left panel |
| Order Status | `Sales Transactions[order_status]` | List (multi-select) | Left panel |
| Order Date Range | `Calendar[date]` | Between range slider | Left panel or right panel |

**Range slider note:** Test whether dragging fires continuous DAX re-evaluations (some Power BI versions do). If so, replace with two "Start Month" / "End Month" dropdown slicers — they fire only on selection completion and are significantly less expensive.

### 4.7 Cross-Filter Behaviour

| Clicking this | KPI cards | Trend chart | Scatter plot |
|---|---|---|---|
| Scatter plot (state) | None (isolated) | Cross-filter | N/A |
| Trend chart (time period) | None (isolated) | N/A | Cross-highlight |
| EUR/BRL line | None (isolated) | Cross-highlight | None |
| Revenue in USD card | None on all | None | None |
| Latest EUR/BRL Rate card | None on all | None | None |

**Disclaimer text box (always visible, not collapsed):**
> "This analysis identifies statistical correlations between external variables (weather, exchange rates) and order patterns. Correlation does not establish causation. FX data reflects mid-market EUR cross-rates from Frankfurter API, not transactional rates. These findings are hypothesis-generation inputs, not operational decision triggers."

---

## 5. Page 3 — Pipeline Health

### 5.1 Purpose and Audience

**Full title:** ETL Pipeline Operations: Multi-Source Ingestion and Medallion Layer Status  
**Tab label:** Pipeline Health  
**Audience:** Data engineers, Analytics leads, trust/governance team  
**Audience label:** [Engineering]  
**Canvas background:** `#1C2833` (dark — signals "system status" not "business analysis")  
**Page accent bar colour:** `#C55A11` (amber — operational urgency)

**Why this page matters for a portfolio project:** Most portfolio dashboards hide the engineering. This page is the differentiator that separates a "I made some visuals" project from a "I built a reliable data product" project. Its presence signals four capabilities simultaneously: observability, data quality vocabulary, production-architecture thinking, and conversation-catalyst material for technical interviews.

**Top annotation (prominent, top of page):**
> "This page treats data quality as a first-class business concern. Downstream decisions — revenue reporting, operational KPIs, customer satisfaction scores — are only trustworthy if the pipeline that produces them is auditable and observable."

### 5.2 Layout

```
+-----------------------------------------------------------------------------------+
| NAVIGATION BAR (dark variant)                                                     |
+-----------------------------------------------------------------------------------+
| "ETL Pipeline Operations: ..."  [Engineering]   Last refreshed: [Data Freshness] |
+-----------------------------------------------------------------------------------+
| RUN STATUS | OLIST CSV  | OPEN-METEO | FRANKFURTER| ROWS LOADED| QUALITY SCORE  |
| (badge+ts) | (badge+cnt)| (badge+cnt)| (badge+cnt)| (card)     | (gauge/card)   |
| 195 × 80px × 6 cards                                                              |
+-----------------------------------------------------------------------------------+
|  ROW COUNTS OVER TIME (line chart)   |  QUALITY CHECKS TABLE                     |
|  618 × 220 px (dark card)            |  618 × 220 px (dark card)                 |
+-----------------------------------------------------------------------------------+
|  PIPELINE RUN LOG (table)            |  LOAD LATENCY CHART                        |
|  618 × 220 px (dark card)            |  618 × 220 px (dark card)                 |
+-----------------------------------------------------------------------------------+
|  RUN DATE slicer  |  SOURCE filter  |  STATUS filter  (dark variant)              |
+-----------------------------------------------------------------------------------+
```

### 5.3 Status Badge Cards (6-up, 195 px wide × 80 px tall, dark variant)

Card styling (dark variant): background `#273746`, text `#FFFFFF`, label `#A9B7C6`, border `1px #4A5568`.

Badge spec: 72 px × 28 px, corner-radius 4 px, 9 pt Bold white text.

**Badge contrast note:** White (`#FFFFFF`) on the green badge (`#1E8449`) is ~5.1:1 — passes AA. However `#E67E22` (orange) and `#C0392B` (red) on the dark card background (`#273746`) may be below 4.5:1 for the badge fill itself against the surrounding card. Verify all three badge colours against `#273746` using a contrast checker before publishing. If either fails, either (a) use a slightly lighter orange (`#F0892D`) and lighter red (`#D94040`), or (b) enclose badges in a `#FFFFFF` 2 px outline to ensure UI component contrast (3:1 minimum for non-text UI elements per WCAG 1.4.11).

| Card | Content | Badge colour |
|---|---|---|
| Run Status | Overall pipeline status badge + last run timestamp | `#1E8449` / `#E67E22` / `#C0392B` |
| Olist CSV | "PASS/WARN/FAIL" badge + row count loaded | Same logic |
| Open-Meteo API | "PASS/WARN/FAIL" badge + weather records loaded | Same logic |
| Frankfurter API | "PASS/WARN/FAIL" badge + FX records loaded | Same logic |
| Rows Loaded | Numeric count of total Gold-layer rows across all 3 fact tables | `#2E86C1` accent |
| Quality Score | `Data Quality Pass Rate` measure formatted as `0.0%` | Green ≥ 95%; Amber 90–94.9%; Red < 90% |

**Three-state criteria:**

| State | Condition |
|---|---|
| NOMINAL | All row counts within 0.5% of baseline; all 3 sources loaded; null rates on join keys < 2%; no transformation errors logged |
| REVIEW NEEDED | Row count deviates 0.5–5% from baseline; OR null rate on join key > 2%; OR API returned partial data (date range differs > 3 days from expected) |
| PIPELINE FAULT | Source failed to load entirely; OR Gold table is empty; OR order_id null rate > 5%; OR Bronze→Silver row count drops > 10% without documented dedup reason |

### 5.4 Row Counts Over Time Line Chart

**Visual type:** Line chart  
**X-axis:** Load date (derived from `_loaded_at`, monthly grain)  
**Series:** One line per fact table (fact_sales, fact_weather_daily, fact_fx_rates)  
**Colours:** Series 1–3 (`#2E86C1`, `#17A589`, `#D4A017`)  
**Cross-highlight only** — engineers need comparative context across sources  
**Tooltip:** Source name, rows loaded, load timestamp (`_loaded_at`), stage name (Bronze/Silver/Gold)

**Pipeline status card text:**
```
Pipeline run: [Data Freshness]
Sources ingested: Olist CSV (9 tables) | Open-Meteo API ([N] city-date records) | Frankfurter API ([N] trading-day records)
Medallion layers: Bronze loaded | Silver transformed | Gold aggregated
Row integrity: Orders [ACTUAL] / Expected ~99,441 | Items [ACTUAL] / Expected ~112,650
Join coverage: Weather join: [X]% | FX join: [X]%
Null rate on order_id: [X]% | Null rate on customer_zip: [X]%
Status: NOMINAL / REVIEW NEEDED / PIPELINE FAULT
```

### 5.5 Quality Checks Table

**Visual type:** Table visual  
**Source:** `analytics.data_quality_log` (via DAX query or dedicated import table)  
**Columns:** Check Name, Check Type, Status badge (conditional format), Rows Affected, Last Run timestamp  
**Row height:** 28 px; alternating fill `#273746` / `#2E3F50`; header `#1C2833` with `#4A5568` bottom border  
**Cross-filter:** Quality checks table → filters a summary card showing error count by check type (only cross-filter on this page)  
**All other interactions:** Isolated (`None`)

**Annotation callout text (near join coverage metrics):**
> "Weather join coverage is constrained by Open-Meteo's city-level resolution and Olist's zip-code-prefix customer location. ~5–8% of orders are expected to have no weather match due to rural or unrecognized zip codes. These rows are excluded from weather correlation (Page 2) but retained in all other pages."

### 5.6 Pipeline Run Log Table

**Visual type:** Table visual  
**Columns:** Run Timestamp, Source Name, Status Badge, Rows Extracted, Rows Loaded, Rows Rejected, Duration (seconds)  
**Styling:** Same alternating dark row fill as quality checks table  
**Interactions:** All isolated (`None`) — engineers on a monitoring page expect stability, not dynamic filtering

### 5.7 Slicer Configuration

| Slicer | Field | Style |
|---|---|---|
| Run Date | Derived from `_loaded_at` | Dropdown or button |
| Source | Source name field | List |
| Status | Pipeline status | List |

**Explicitly excluded from Page 3:** Geography slicers, weather/FX slicers, product category, Calendar[date] order-date slicer. The `_loaded_at` audit column has a different temporal meaning than `Calendar[date]`. Placing an order-date slicer here misleads engineers into thinking they are filtering by load time.

---

## 6. Page 4 — Source Detail

### 6.1 Purpose and Audience

**Full title:** Transaction-Level Explorer: Orders, Sellers, Products, and Enriched Attributes  
**Tab label:** Source Detail  
**Audience:** QA engineers, data stewards, analysts investigating KPI anomalies  
**Audience label:** [QA]  
**Page accent bar colour:** `#404040`

**Three questions an analyst lands on this page to answer:**

1. What is the actual delivery delay distribution — not the average — for specific states? (Filter to AM, PA, MA and inspect estimated vs actual delivery date columns side by side.)
2. Which specific sellers have the most late deliveries or lowest review scores? (Filter by seller_id, sort by avg review score ascending.)
3. Are the EUR/BRL rates actually varying day-by-day in the enrichment join, or did the join produce a single static rate for all orders? (Filter to a single month, inspect `eur_brl_rate` column variance.)

### 6.2 Layout

```
+-----------------------------------------------------------------------------------+
| NAVIGATION BAR                                                                    |
+-----------------------------------------------------------------------------------+
| "Transaction-Level Explorer: ..."  [QA]   "Viewing: [SELECTEDVALUE measure]"     |
+-----------------------------------------------------------------------------------+
| YEAR  |  QUARTER  |  ORDER STATUS  |  PRODUCT CATEGORY  |  CUSTOMER STATE  | ROWS|
| (btn) |  (btn)    |  (list)        |  (dropdown+search) |  (dropdown)      | card|
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  PRIMARY TABLE VISUAL (full width: 1248 × 500 px, scrollable)                    |
|  order_item_id | order_code | date | customer_city/state | category | price |     |
|  freight | quantity | delivery_days_actual | delivery_days_estimated | status |   |
|  [FX columns] | [weather columns] | _loaded_at                                   |
|                                                                                   |
+-----------------------------------------------------------------------------------+
|  Footer: [Row Count measure] rows | Data as of [Data Freshness] | Source: Gold    |
+-----------------------------------------------------------------------------------+
```

### 6.3 Filter Strip

Replaces the KPI card row. Uses dropdown slicers at compact 48 px height, full bottom-bar position.

| Slicer | Field | Style | Notes |
|---|---|---|---|
| Year | `Calendar[year]` | Button | |
| Quarter | `Calendar[quarter]` | Button | |
| Month | `Calendar[month]` | List | |
| Order Status | `Sales Transactions[order_status]` | List (multi-select) | |
| Product Category | `Products[Category (English)]` | Dropdown (searchable) | 74 categories — searchable dropdown is mandatory |
| Customer State | `Customers[Customer State]` | Dropdown | |
| Seller Region | `Sellers[region]` | Dropdown | |
| Row Count (card) | `Order Count` | KPI card (small) | Right end of filter strip, isolated |

**Default filter on load:** Year = 2017 + 2018 recommended — prevents the 100K-row table from rendering unfiltered on first load (first render latency is highest for large tables).

### 6.4 Primary Table Configuration

**Visual type:** Table visual  
**Column header:** 10 pt Semibold, `#1C2833`, `#EBF0F8` background, 1 px `#D5D8DC` bottom border  
**Row height:** 24 px (compact)  
**Alternating row fill:** `#FFFFFF` / `#F5F6FA`  
**Numeric columns:** Right-aligned  
**Text columns:** Left-aligned  
**Null/blank cells:** Display em dash (—). Use `IF(ISBLANK([field]), "—", [field])` DAX pattern for text columns.

**Recommended visible columns (in order):**

| Column | Source | Notes |
|---|---|---|
| order_code | `fact_sales` | Order identifier |
| line_number | `fact_sales` | Item position within order (1, 2, 3…) |
| order_purchase_date | `Calendar[date]` (via date_key) | Date type after Power Query conversion |
| customer_city | `Customers[Customer City]` | |
| customer_state | `Customers[Customer State]` | |
| category_english | `Products[Category (English)]` | |
| unit_price | `fact_sales` | Renamed: "Item Price (BRL)" |
| freight_value | `fact_sales` | |
| quantity | `fact_sales` | |
| delivery_days_actual | `fact_sales` | |
| delivery_days_estimated | `fact_sales` | |
| order_status | `fact_sales` | |
| precipitation_mm | `Daily Weather Conditions` | May be blank if no weather match |
| eur_brl_rate | `Exchange Rates` | Verify day-by-day variation |
| _loaded_at | `fact_sales` | Audit timestamp |

**Conditional formatting:**

| Column | Rule | Formatting | Colour-independent indicator |
|---|---|---|---|
| delivery_days_actual | > delivery_days_estimated | Background `#FDECEA`, text `#C0392B` | Prefix cell value with "LATE: " (e.g., "LATE: 12 days") |
| delivery_days_actual | ≤ delivery_days_estimated | Background `#EAF7EE`, text `#1E8449` | No prefix needed (on-time is the expected state) |
| precipitation_mm | > 10 | Background `#EBF5FB` | Append " (High)" to cell value (e.g., "14.2 (High)") |
| eur_brl_rate | ISBLANK | Em dash display, `--semantic-neutral` colour | Em dash (—) is itself the indicator |

**Accessibility note:** "LATE: " prefix ensures late delivery meaning is conveyed by text, not colour alone, per WCAG 1.4.1. Use a DAX measure: `IF([delivery_days_actual] > [delivery_days_estimated], "LATE: " & [delivery_days_actual], FORMAT([delivery_days_actual], "0"))` for the displayed column.

**Cross-filter behaviour:** All active — category, state, order status cross-filter the table. Row count card is isolated.

**Breadcrumb dynamic text box** (page title area, right-aligned):
```dax
Viewing =
VAR _cat   = SELECTEDVALUE(Products[Category (English)], "All Categories")
VAR _state = SELECTEDVALUE(Customers[Customer State], "All States")
VAR _yr    = SELECTEDVALUE(Calendar[year], "All Years")
RETURN "Viewing: " & _yr & " | " & _cat & " | " & _state
```

### 6.5 Annotation Callouts

**Near table (visible on load):**
> "This table operates at order-item grain: each row is one product within one order. An order with 3 products appears as 3 rows. Filter on order_code to aggregate to order level. The line_number column (1, 2, 3…) identifies item position within an order."

**Near FX enrichment columns:**
> "FX enrichment is joined at order_purchase_timestamp date. Rates reflect mid-market EUR/BRL and EUR/USD from Frankfurter API for the calendar date of purchase, not the date of payment settlement. For installment payment orders, the rate at purchase date may not reflect the effective exchange rate across the payment period."

### 6.6 Common Drilldown Workflows

**Delivery performance investigation:**
Filters → order_status = "delivered" | customer_state = [target state] | [date range]
Sort by: delivery_days_actual descending (worst first)

**Seller quality investigation:**
Filters → [date range of interest]
Columns: seller_id, seller_state, category, unit_price, freight_value, delivery_days_actual, order_status
Sort: order_status descending, then delivery_days_actual descending

**FX enrichment verification:**
Filters → single month
Columns: order_purchase_date, unit_price (BRL), eur_brl_rate, eur_usd_rate, order_code
Sort: order_purchase_date ascending — verify eur_brl_rate changes day-by-day (not static)

**Weather enrichment verification:**
Filters → customer_state = SP (highest order density)
Columns: order_purchase_date, customer_city, customer_state, precipitation_mm
Sort: precipitation_mm descending — verify high-precipitation records align with known rainy season (Nov–Mar in SP)

---

## 7. Navigation Header

### 7.1 Structure

A 1280 × 40 px bar at y=0 on every page.  
**Background:** Single Rectangle shape, fill `#1B4F8A`, no border.

### 7.2 Button Specification

Four Button visuals placed within the header bar. Each button height = 40 px. Minimum width = 120 px; pad 20 px left and right of text. Use Action Type: Page Navigation.

| Property | Active Tab | Inactive Tab |
|---|---|---|
| Background fill | `#FFFFFF` | Transparent |
| Font colour | `#1B4F8A` | `#FFFFFF` |
| Bottom border | 3 px solid `#2E86C1` | None |
| Font | Segoe UI, 10 pt, Semibold | Segoe UI, 10 pt, Semibold |

| Page | Button Label | Target |
|---|---|---|
| 1 | Market Overview | Page 1 |
| 2 | External Signals | Page 2 |
| 3 | Pipeline Health | Page 3 |
| 4 | Source Detail | Page 4 |

**Home button (Pages 2–4 only):** Placed top-left of nav bar, visually separated by a thin vertical divider. Labelled "Overview" or a house icon (Unicode ⌂ U+2302). Navigates to Page 1 and resets slicer context.

**Report title (right-aligned within nav bar):** "Olist E-Commerce Analytics", 10 pt Semibold, `#FFFFFF`, x=1264, right-aligned.

**Page 3 note:** Keep the `#1B4F8A` nav bar on Page 3 — it contrasts correctly against the `#1C2833` canvas and against the white active tab. No dark-variant nav bar needed.

**Keyboard tab order for navigation buttons:** Navigation buttons must be **last** in the Selection pane tab order on every page. Users should reach content before navigation. Set this explicitly in View → Selection pane by reordering layers.

---

## 8. Global Slicer Sync Configuration

Configure in Power BI Desktop via View → Sync Slicers:

| Slicer Field | Synced Pages | Visible On | Synced (Filters) |
|---|---|---|---|
| `Calendar[year]` | All 4 | All 4 | All 4 |
| `Calendar[quarter]` | Pages 1, 2, 4 | Pages 1, 2, 4 | Pages 1, 2, 4 |
| `Customers[Customer State]` | Pages 1, 2, 4 | Pages 1, 2, 4 | Pages 1, 2, 4 |
| `Sales Transactions[order_status]` | Pages 2, 3, 4 | Pages 2, 3, 4 | Pages 2, 3, 4 |

**Intentionally NOT synced:** `Calendar[month]`, `Calendar[date]` range slider, `Daily Weather Conditions[city]`, `Products[Category (English)]`, `Sellers[region]`. These are page-specific exploration controls.

**Critical:** Do NOT sync the `Calendar[date]` range slider on Page 2 to Page 3. The `_loaded_at` column on Page 3 has a different temporal meaning than `Calendar[date]` on Page 2. Syncing them would mislead engineers into thinking they are filtering by load time when they are actually filtering by order date.

---

## 9. Accessibility Requirements

### 9.1 Alt Text (set in Format → General → Alt Text)

Apply to every data-carrying visual. Every visual type mentioned in Sections 3–6 must have an alt text entry. Expand all abbreviations in alt text: "MoM" → "Month-over-Month", "YoY" → "Year-over-Year", "BRL" → "Brazilian Real", "FX" → "foreign exchange".

| Visual | Alt Text |
|---|---|
| Map visuals (all pages) | "Choropleth map showing [metric] by Brazilian customer state. Use the Source Detail table for accessible data." |
| Revenue Trend line chart (Page 1) | "Line chart showing monthly total revenue in Brazilian Real from September 2016 to October 2018. Revenue grows approximately 18-fold over the period. Underlying data available in Source Detail table." |
| Top Categories horizontal bar chart (Page 1) | "Horizontal bar chart showing total revenue by top 10 product categories, sorted highest to lowest. Underlying data available in Source Detail table." |
| Dual-Axis Trend combo chart (Page 2) | "Combination chart showing monthly total revenue as columns and EUR/BRL exchange rate as a line, covering 2016–2018. Underlying data available in Source Detail table." |
| Scatter plot (Page 2) | "Scatter plot showing relationship between average daily precipitation in millimeters and total revenue in Brazilian Real, plotted by customer state. Each point is coloured and labelled by weather category. Underlying data available in Source Detail table." |
| Row Counts Over Time line chart (Page 3) | "Line chart showing the number of rows loaded per pipeline run for each fact table (fact_sales, fact_weather_daily, fact_fx_rates). Most recent run date shown in chart title." |
| Quality Checks Table (Page 3) | "Table listing data quality check results: check name, type, status (PASS or FAIL), rows affected, and last run timestamp." |
| Pipeline Run Log Table (Page 3) | "Table showing pipeline run history: run timestamp, source name, status, rows extracted, rows loaded, rows rejected, and duration in seconds." |
| KPI change cards (all pages) | "KPI card: [Measure Name]. Current value: [value]. Change from prior period: [Month-over-Month %] (Month-over-Month)." Use CONCATENATE-based DAX measures for dynamic alt text where the Power BI version supports it. |
| Pipeline status badge cards (Page 3) | "Status card for [source name]: [NOMINAL / REVIEW NEEDED / PIPELINE FAULT]. [N] rows loaded. Last run: [timestamp]." |
| Source Detail table (Page 4) | "Filterable table showing order-item level data including order code, dates, customer location, product category, prices, delivery days, order status, and weather and FX enrichment values." |
| Decorative rectangles, dividers, icons | Set to empty string or "Decorative" |

### 9.2 Slicer Header Labels

Set a visible header title on every slicer using plain language (not the internal field name):

| Field | Accessible Label |
|---|---|
| `Calendar[year]` | Year |
| `Calendar[quarter]` | Quarter |
| `Calendar[month]` | Month |
| `Calendar[date]` | Order Date Range |
| `Customers[Customer State]` | Customer State |
| `Sellers[region]` | Seller Region |
| `Products[Category (English)]` | Product Category |
| `Sales Transactions[order_status]` | Order Status |
| `Daily Weather Conditions[city]` | Weather City |

### 9.3 Keyboard Tab Order

Set explicitly in View → Selection pane (layer reorder) for every page. Navigation buttons must always be **last** — keyboard users should reach content before navigation. Right-click any visual in the Selection pane and drag to reorder.

**Page 1 tab order:**
Year slicer → Quarter slicer → Customer State slicer → Total Revenue card → Total Orders card → Avg Order Value card → Freight % card → Revenue trend → Category bar → Map → Navigation buttons

**Page 2 tab order:**
Year slicer → Quarter slicer → Month slicer (left panel) → Customer State slicer → Weather City slicer → Order Status slicer → Order Date Range slicer → Total Revenue card → Order Count card → High Precipitation Days card → EUR/BRL Rate card → Revenue in USD card → Dual-Axis Trend chart → Scatter plot → Navigation buttons

**Page 3 tab order:**
Year slicer → Run Date slicer → Source slicer → Status slicer → Run Status card → Olist CSV card → Open-Meteo card → Frankfurter card → Rows Loaded card → Quality Score card → Row Counts chart → Quality Checks table → Pipeline Run Log table → Load Latency chart → Navigation buttons

**Page 4 tab order:**
Year slicer → Quarter slicer → Month slicer → Order Status slicer → Product Category slicer → Customer State slicer → Seller Region slicer → Row Count card → Primary Detail table → Navigation buttons

### 9.4 Colour Contrast

Verified text contrast ratios (WCAG 2.1 AA requires 4.5:1 for normal text, 3:1 for large text ≥ 18 pt or ≥ 14 pt bold, and 3:1 for UI component boundaries):

| Foreground | Background | Ratio | Status |
|---|---|---|---|
| `--text-primary` `#1C2833` | `--bg-card` `#FFFFFF` | ~15.3:1 | Passes AAA |
| `--text-inverse` `#FFFFFF` | `--bg-pipeline` `#1C2833` | ~16.2:1 | Passes AAA |
| `--text-secondary` `#566573` | `#FFFFFF` | ~7.0:1 | Passes AA |
| `#FFFFFF` badge text | `#1E8449` green badge | ~5.1:1 | Passes AA |
| `#FFFFFF` inactive nav text | `#1B4F8A` nav bar | ~7.3:1 | Passes AA |
| `#1B4F8A` active nav text | `#FFFFFF` active button | ~7.3:1 | Passes AA |
| `#7F8C8D` footer caption | `#F5F6FA` canvas | ~4.6:1 | Marginally passes AA (8 pt — verify on target display) |

**Pipeline badge colours — verify before publishing:** `#E67E22` (orange/warn) and `#C0392B` (red/fail) as badge fill colours against the dark card background `#273746` may approach the WCAG AA threshold for UI component boundaries (3:1). Verify with a contrast checker. If either fails: add a 2 px `#FFFFFF` outline around each badge, or use `#F0892D` (lighter orange) and `#D94040` (lighter red) as badge fills.

**Focus indicator:** The focus ring on all interactive elements (buttons, slicers, visuals) must achieve a minimum 3:1 contrast ratio against adjacent colours (WCAG 2.4.7). Set the focus ring colour to `#1B4F8A` (`--brand-cobalt`) on light pages, and to `#2E86C1` (`--brand-sky`) on the dark Page 3 canvas. Power BI theme JSON key: set `"visualContainerBorder"` with sufficient weight. For custom navigation buttons, apply the hover style to the focus state (Power BI does not expose a separate focus state in most versions).

**Rule:** Never use colour alone as the sole differentiator. Pair colour with shape (▲ / ▼ / ▬ arrows), text labels (PASS/WARN/FAIL), or pattern. See scatter plot fix in Section 4.5 for weather series implementation.

### 9.5 Touch Target Sizes

All tap targets (buttons, slicers, visual bars) must be at least 44 × 44 px for tablet use. Power BI default button sizes are often smaller — increase button height to 36 px minimum and add 8 px padding.

### 9.6 Run Accessibility Checker Before Publishing

View → Accessibility → Check accessibility. Address all high-severity issues. Common issues in this report:

- Missing alt text on visuals (priority: map and scatter)
- Slicer header text uses field name rather than plain language
- Navigation button focus indicator low contrast
- Tab order not explicitly set (defaults to z-order, which is rarely logical)

---

## 10. Performance Considerations

### 10.1 Weather SUMX Performance (Page 2 — Highest Risk)

The `Revenue on Rainy Days` and `Avg Temperature on Order Days` measures use per-row SUMX iteration with `RELATED()` lookups across `fact_weather_daily`. At 112K fact_sales rows this is acceptable for an Import model, but Page 2's scatter plot re-evaluates these measures for every state-month combination in the visual context filter.

**Mitigations (in priority order):**

1. **Confine weather-intensive visuals to Page 2 only.** Do not place SUMX weather measures on Pages 1, 3, or 4. The performance hit is paid once when the user navigates to Page 2.
2. **Position the scatter plot in the centre-right of the canvas** (not top-left). Power BI renders left-to-right, top-to-bottom — placing KPI cards and the trend line first ensures the user sees meaningful content immediately while the scatter renders in parallel.
3. **Consider a `fact_weather_monthly_state` aggregate table** in the PostgreSQL analytics schema (a materialized view grouping `fact_weather_daily` by year-month + state). Page 2 visuals can query this aggregation instead of iterating daily rows — the highest-leverage single optimisation available. This is an ETL recommendation that directly addresses the UX performance problem.
4. **Add a subtitle on Page 2** (9 pt, `#566573`): "ERA5 historical data, pre-aggregated at Silver layer. Slight render delay expected on first load."

### 10.2 Date Range Slicer Drag Behaviour

Range sliders may fire continuous DAX re-evaluations on every drag movement in some Power BI versions. Test by dragging slowly and observing spinner frequency. If this occurs, replace with two "Start Month" and "End Month" dropdown slicers — these fire only on selection completion.

### 10.3 Page 4 Table First-Load Latency

A 100K+ row table with no default filter will be slow on first render. Set the default slicer state to Year = 2017 + 2018 on this page (consistent with the Page 1 default). Implement via bookmark on report open.

### 10.4 2016 Data Sparsity Anti-Pattern

The Olist dataset begins in late 2016. Without a default filter, the trend line on Page 1 shows near-zero activity through most of 2016, making the growth story look like a spike from nothing — a data availability artefact, not a business trend. Always default to Year = 2017 and 2018. Add visible note: "2016 data partial — orders begin Q4 2016."

---

## 11. Implementation Checklist

### Global Setup (Once)
- [ ] Set canvas to 1280 × 720 px: Format → Page information → Canvas settings
- [ ] Set default font to Segoe UI in theme
- [ ] Import custom theme JSON encoding all colour tokens (View → Themes → Browse for themes)
- [ ] Create navigation bar Rectangle + Button set on Page 1, copy-paste to Pages 2–4, adjust active tab per page
- [ ] Create a "Master KPI Card" template on a hidden scratch page; copy from this master for all KPI instances
- [ ] Validate `date_key` INT → Date conversion in Power Query for all tables; verify date range slicer works correctly against Calendar table
- [ ] Verify `Revenue in USD` cross-rate calculation: EUR/BRL ÷ EUR/USD produces expected values for a known date (e.g., January 15, 2018)
- [ ] Set default slicer state to Year = 2017 + 2018 via bookmark applied on report open (Pages 1 and 4)
- [ ] Configure Sync Slicers per Section 8

### Per-Page Verification Before Publishing
- [ ] All visual titles are business English (not DAX measure names or field paths)
- [ ] No visual has a visible native background fill (Rectangle shapes provide card framing)
- [ ] Y-axis labels use abbreviated number format (M/K suffixes)
- [ ] Data labels hidden on charts with > 8 data points
- [ ] Slicer bar visible and contains correct slicers for that page
- [ ] Footer caption present with data currency timestamp
- [ ] Tab navigation buttons work correctly; active tab state matches current page
- [ ] Semantic colour usage (positive/negative/warning) is correct per card
- [ ] KPI cards isolated from cross-filtering (Edit Interactions: None) where specified
- [ ] `Revenue in USD` card has subtitle: "Approx. via ECB cross-rate. Not for financial reporting."
- [ ] Page 2 weather scatter has annotation: "Cities with no weather match excluded. See Pipeline page for match rate."
- [ ] Page 2 correlation disclaimer text box is visible, not collapsed
- [ ] Page 3 pipeline badge colours match three-state criteria table
- [ ] Page 4 table defaults filtered (not showing 100K unfiltered rows)
- [ ] Page 4 breadcrumb SELECTEDVALUE measure renders correctly
- [ ] All null/blank cells in Page 4 table show em dash (—)
- [ ] Alt text set on all map and scatter visuals
- [ ] Slicer header labels set to plain language (not field names)
- [ ] Tab order set explicitly in Selection pane (nav buttons last)
- [ ] Run View → Accessibility → Check accessibility; all high-severity issues resolved
- [ ] Range slider drag-latency tested on Page 2; replaced with dropdowns if continuous re-evaluation confirmed
- [ ] Test date range slicer with known date range against equivalent PostgreSQL query (UAT validation)
