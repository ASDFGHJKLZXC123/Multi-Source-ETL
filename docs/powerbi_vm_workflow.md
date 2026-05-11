# Power BI on macOS — Parallels VM Workflow

Power BI Desktop is Windows-only. For macOS authoring, run it inside a Windows
VM on Parallels (or any other macOS hypervisor) and connect to the Postgres
container running on the host.

## Prerequisites

- Docker Desktop on macOS, with the pipeline already loaded:
  `make bootstrap` (or `make full-refresh` if `init` and `setup` have been run).
- Parallels Desktop with a Windows 10/11 VM.
- Power BI Desktop (latest) installed inside the VM.
- Npgsql 6.0.x (Power BI's PostgreSQL provider) installed inside the VM.

## Step 1 — Make the host's Postgres reachable from the VM

`docker-compose.yml` publishes the container's `5432` on the macOS host as
**`5433`** (`ports: "5433:5432"`). From inside the Windows VM the macOS host is
reachable as `host.docker.internal` (Parallels shared networking) or via the
Mac's LAN IP. Verify with:

```powershell
Test-NetConnection -ComputerName host.docker.internal -Port 5433
```

If that fails, fall back to the Mac's LAN IP (`ifconfig | grep 'inet '` on
macOS, then use that IP in Power BI).

## Step 2 — Connect from Power BI

`Get Data → PostgreSQL database`

| Field | Value |
|---|---|
| Server | `host.docker.internal:5433` (or the Mac's LAN IP, port 5433) |
| Database | `etl_pipeline` |
| Data Connectivity mode | **Import** |
| Advanced Options → Command timeout | `0` (default) |
| Advanced Options → SQL statement | leave blank (use Navigator) |
| Include relationship columns | unchecked |

Authentication: **Database** → user `powerbi_reader`, password from `.env`
(`POSTGRES_PBI_READER_PASSWORD`, defined in `06_powerbi_readiness.sql`). The
role is `SELECT`-only on the `analytics` schema.

In the Navigator, tick:

- `analytics.dim_date`
- `analytics.dim_customer`
- `analytics.dim_product`
- `analytics.dim_store`
- `analytics.dim_currency`
- `analytics.fact_sales`
- `analytics.fact_weather_daily`
- `analytics.fact_fx_rates`
- (optional) `analytics.v_sales_enriched` — pre-joined view for ad-hoc analysis

## Step 3 — Build the model

Follow `docs/stage8_powerbi.md` for the model design (relationships,
role-playing currency dimension, `date_key` → `Date` conversion in Power Query),
then `docs/stage9_dax_measures.md` for the 27 measures and
`docs/stage10_dashboard_pages.md` for the 4 page layouts.

## Step 4 — Save & export

Save the `.pbix` to `pbix/multi_source_etl.pbix` (git-ignored — share via
OneDrive / direct send rather than committing).

Export each page to PNG via `File → Export → Export to PDF` then split, or use
the Power BI page screenshot menu, and place the PNGs in `docs/screenshots/`
following the naming convention noted there.
