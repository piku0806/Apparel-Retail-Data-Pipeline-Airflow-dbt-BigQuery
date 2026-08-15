# Apparel Retail Pipeline — Airflow + dbt + BigQuery

An end-to-end data engineering project. Synthetic apparel retail data (sales,
customers, products, stores) is generated in batches, loaded into BigQuery,
and transformed into a dimensional model using **Apache Airflow** for
orchestration and **dbt** for SQL transformation.

It uses the source data generator and data-quality characteristics, applying
data engineering problem set like CDC-based dimension upserts, referential
integrity enforcement, and dirty-data handling to a different, widely used
toolchain: Airflow and dbt on a cloud data warehouse(BigQuery)

## Architecture

```
                       ┌─────────────────────────┐
                       │   generator/*.py         │   pandas + Faker, adapted
                       │   (batch mode, derived   │   from a Databricks/Spark
                       │   from a Spark streaming │   streaming generator
                       │   job)                   │
                       └────────────┬─────────────┘
                                    │ CSV per batch
                                    ▼
                       data/raw/{customers,products,stores,sales}/*.csv
                                    │
                                    │ scripts/load_to_bigquery.py (WRITE_APPEND)
                                    ▼
                 BigQuery: apparel_raw.{customers,products,stores,sales}_raw
                                    │
                                    │ dbt staging models (cast/clean, flag DQ issues)
                                    ▼
                 BigQuery: apparel_analytics_staging.stg_*
                                    │
                                    │ dbt marts (incremental + merge, SCD Type 1)
                                    ▼
     BigQuery: apparel_analytics.{dim_customers, dim_products, dim_stores, fct_sales}
```

The full pipeline is orchestrated by a single Airflow DAG:
`dags/apparel_pipeline_dag.py`.

## Design rationale

**Batch generation instead of streaming.** The source generator this project
is adapted from ran four infinite loops inside a Spark cluster, writing to
Delta Lake every `BATCH_INTERVAL_S` seconds. Airflow tasks are designed to
run to completion and exit rather than run indefinitely, so each
`generate_*_stream` function was converted into a `generate_*_batch()`
function that produces exactly one batch per invocation. The DAG's schedule
(15-minute interval by default) replaces the original continuous-loop
cadence.

**Referential integrity enforced across tasks, not within a single process.**
The original generator held `existing_customers`, `existing_products`, and
`existing_stores` in memory and withheld sales generation until all three had
data. Because each Airflow task runs as a separate process, this check is
now performed by querying BigQuery directly (`generator/state.py`), and the
DAG enforces ordering through explicit task dependencies: `generate_sales`
only executes after all three dimension batches have been generated and
loaded into BigQuery.

**The same data-quality issues are preserved intentionally.** Negative
quantities (returns), negative discounts (invalid data), missing emails and
managers, and CDC-style duplicate rows carrying an updated
`last_update_time` are all still produced by the generator. The dbt project
is built to detect and handle each of these:

| Issue | Handling |
|---|---|
| CDC updates (same ID, new attributes) | `dim_customers` / `dim_products` / `dim_stores` use incremental models with a `merge` strategy on the business key, and `QUALIFY ROW_NUMBER()` to collapse multiple updates within a single batch |
| Missing email / manager | Flagged via `is_missing_email` / `is_missing_manager` columns in staging; not treated as a test failure, since the value is valid |
| Negative discount / price | Flagged via `has_invalid_discount` / `has_invalid_price`, and surfaced with a warn-severity `dbt_expectations` test so it is visible without failing the pipeline |
| Returns (negative quantity) | Flagged via `is_return`; treated as a legitimate business event, not an error |
| Referential integrity | `relationships` tests defined in both `staging/schema.yml` and `marts/schema.yml` (fact table to each dimension) |
| Event-time skew / late-arriving data | `fct_sales` uses the monotonic `transaction_id` as its incremental cursor rather than `event_time`, to avoid silently dropping late-arriving rows — see the comment in `fct_sales.sql` for the full rationale |

**dbt merge in place of a manual PySpark merge.** The Databricks version of
this pattern implements the old/new record split and `DeltaTable.merge()`
call directly in a parametrized notebook. This project expresses the same
SCD Type 1 behavior idiomatically through dbt's `incremental` materialization
with a `merge` strategy — functionally equivalent, with less custom code, and
consistent with common practice in dbt/BigQuery implementations.

## Data reference

All four streams are generated with Faker and re-created on every DAG run,
so exact values vary between runs; the structure, value ranges, and
intentional data-quality characteristics remain consistent. The example rows
and statistics below are drawn from an actual local run of the generator.

### `customers_raw` / `dim_customers`

One row per customer. A customer is re-emitted with a new
`last_update_time` whenever an update is simulated (CDC).

| Column | Description |
|---|---|
| `customer_id` | Sequential integer key; monotonically increasing, never reused |
| `name`, `email`, `address`, `phone_number` | Faker-generated |
| `email` | Approximately 2% NULL by design, representing customers who did not provide an email address. Flagged as `is_missing_email` in staging rather than dropped |
| `age` | 18–70 |
| `gender` | `Male`, `Female`, or `Other` |
| `loyalty_points` | 0–1000 |
| `join_date` | Up to 365 days prior to the row's timestamp |
| `last_update_time` | Incremental (CDC) cursor used by `dim_customers`; back-dated by up to 60 seconds to simulate arrival latency |

Example row: `1,Alexander Romero,taylor12@example.com,"956 Rivera Lakes, North Andrew, IA 43897",2026-01-14T02:31:20Z,648,(659)406-7221x6351,30,Other,2026-08-15T02:31:20Z`

### `products_raw` / `dim_products`

One row per SKU, following the same CDC re-emission pattern as customers.

| Column | Description |
|---|---|
| `product_id` | Sequential integer key |
| `name` | Procedurally combined as `<word> <color> <size>`, e.g. `Order LightGray S` — not intended to resemble a realistic product name |
| `category` | One of `Casual Wear`, `Formal Wear`, `Sportswear`, `Accessories`, `Footwear` |
| `brand` | One of 20 fictional brand names, e.g. `Elemental Gear`, `Summit Outfitters` |
| `size` | `XS`–`XL` |
| `price` | $10–$150 (observed range in a sample run: $16.80–$142.95) |
| `stock_quantity` | 0–200 |
| `last_update_time` | Same role as in `customers_raw`; a repeated `product_id` with a new price, stock level, or description represents a price change or restock event |

Example row: `1,Order LightGray S,Accessories,Elemental Gear,20.85,171,S,LightGray,"Kid quite car think check mission its especially.",2026-08-15T02:31:21Z`

### `stores_raw` / `dim_stores`

One row per store location.

| Column | Description |
|---|---|
| `store_id` | Sequential integer key |
| `name` | Formatted as `Store <id> - <city>` |
| `status` | `Open` or `Under Renovation` |
| `manager` | Approximately 2% NULL by design, representing a vacant manager position. Flagged as `is_missing_manager` rather than dropped |
| `open_date` | 1–5 years prior to the row's timestamp |

`STORE_BATCH_SIZE` permits zero new or updated stores in a given run, since
store openings occur far less frequently than customer signups or product
restocks. The `load_stores_to_bq` DAG task handles this case as a no-op
rather than a failure.

Example row: `1,Store 1 - North Eileen,"78586 Amber Crossing, New Matthewview, MT 54394",Nicole Sanchez,2024-11-06T02:31:22Z,Open,839.615.3788x90616,2026-08-15T02:31:22Z`

### `sales_raw` / `fct_sales`

One row per transaction. Unlike the three dimension streams, a sales record
does not mutate once written.

| Column | Description |
|---|---|
| `transaction_id` | Sequential integer key, monotonically increasing across all runs (the generator resumes from `max(transaction_id) + 1` on each invocation). This property is also why `fct_sales` uses `transaction_id`, rather than `event_time`, as its incremental cursor — see `fct_sales.sql` |
| `customer_id`, `product_id`, `store_id` | Foreign keys, always drawn from IDs already present in the corresponding dimension. Referential integrity is enforced at generation time by querying BigQuery for existing IDs before selection |
| `quantity` | Normally 1–5; approximately 5% of rows are negative, representing a return. Flagged via `is_return` and treated as a legitimate business event, not an error |
| `unit_price` | $10–$100 |
| `total_amount` | `quantity * unit_price` |
| `tax_amount` | `total_amount * 8%` |
| `discount_applied` | Normally $0–$20; approximately 2% of rows are negative, which represents invalid data (a discount should never be negative). Flagged via `has_invalid_discount` and surfaced through a warn-severity dbt test rather than being dropped or silently ignored |
| `payment_method` | One of `Cash`, `Credit Card`, `Debit Card`, `Mobile Pay`, `Gift Card` |
| `event_time` | Back-dated by up to 60 seconds from generation time, simulating out-of-order arrival |

Example row: `1,3,2026-08-15T02:30:24Z,14,5,2,66.09,132.18,Mobile Pay,5.73,10.57` —
transaction 1, at store 3, customer 14 purchased 2 units of product 5 at
$66.09 each, paid by Mobile Pay, with a $5.73 discount applied.

In a 500-row sample batch, 29 rows (5.8%) were returns and 7 rows (1.4%) had
an invalid negative discount, consistent with the generator's designed
injection rates of approximately 5% and 2% respectively.

## Project layout

```
generator/                  Batch data generator (adapted from the Spark streaming script)
  config.py                 Shared settings (counts, paths, GCP project/dataset)
  state.py                  Loads existing IDs from BigQuery (referential integrity)
  generate_batch.py         The four generate_*_batch() functions and CLI entry point
scripts/
  load_to_bigquery.py       Appends the latest generated CSV into a BigQuery raw table
dags/
  apparel_pipeline_dag.py   The Airflow DAG
dbt/apparel_dbt/
  models/staging/           Source definitions and cleaning models (one per raw table)
  models/marts/             dim_customers, dim_products, dim_stores, fct_sales
  dbt_project.yml
  packages.yml              dbt_utils, dbt_expectations
  profiles_example.yml      Template for profiles.yml (or ~/.dbt/profiles.yml)
docker-compose.yml           Local Airflow environment (LocalExecutor + Postgres)
requirements.txt
.env.example
```

## Setup

### 1. GCP / BigQuery

1. Create or select a GCP project and enable the BigQuery API.
2. Create a service account with the **BigQuery Data Editor** and
   **BigQuery Job User** roles, and download its JSON key.
3. Place the key at `keys/bq_service_account.json` (this path is
   git-ignored).
4. Copy `.env.example` to `.env` and set `GCP_PROJECT`. The raw dataset
   (`apparel_raw`) is created automatically by the loader on first run, and
   the analytics datasets (`apparel_analytics`, `apparel_analytics_staging`)
   are created automatically by dbt on the first `dbt run`.

### 2. dbt profile

```bash
cp dbt/apparel_dbt/profiles_example.yml dbt/apparel_dbt/profiles.yml
# Edit dbt/apparel_dbt/profiles.yml: set the project and keyfile path.
```

### 3. Run Airflow locally

```bash
docker compose up airflow-init   # One-time: migrate the metadata DB, create admin user (admin/admin)
docker compose up                # Start Postgres, webserver, and scheduler
```

Open http://localhost:8080 (admin/admin), unpause `apparel_pipeline`, and
trigger a manual run. The first run bootstraps the initial customer, product,
and store population; subsequent runs produce incremental batches containing
a mix of new rows and CDC-style updates.

### 4. Run components locally without Docker (optional, for development)

```bash
pip install -r requirements.txt
export PYTHONPATH=$(pwd)
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/keys/bq_service_account.json
export GCP_PROJECT=your-gcp-project

python -m generator.generate_batch customers
python -m generator.generate_batch products
python -m generator.generate_batch stores
python -m generator.generate_batch sales      # Requires the three above to be loaded first

python -m scripts.load_to_bigquery customers
python -m scripts.load_to_bigquery products
python -m scripts.load_to_bigquery stores
python -m scripts.load_to_bigquery sales

cd dbt/apparel_dbt
dbt deps
dbt run
dbt test
```

Without GCP credentials configured, the generator still runs standalone by
falling back to a local JSON cache in `data/state/` for ID lookups, allowing
the generation logic to be verified before BigQuery is configured.

## Potential extensions

- **Replace the local CSV landing zone with GCS.** Point `generator/config.py`
  at a `gs://` bucket and use `GCSToBigQueryOperator` in place of the
  local-file `load_to_bigquery.py` script, more closely matching a
  production landing zone.
- **Adopt `astronomer-cosmos`** to represent each dbt model as an individual
  Airflow task, in place of a single `BashOperator` per `dbt run` / `dbt
  test` invocation, enabling finer-grained retries and lineage visibility in
  the Airflow UI.
- **Add Great Expectations or Soda** as an explicit data-quality gate between
  the raw load and the dbt run, rather than relying solely on dbt tests.
- **Introduce surrogate keys.** The marts currently reuse the generator's
  natural integer IDs directly. To mirror the Databricks project's
  surrogate-key pattern, add `dbt_utils.generate_surrogate_key` to each
  dimension model along with a corresponding lookup join in `fct_sales`.
