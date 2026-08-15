# Apparel Retail Pipeline — Airflow + dbt + BigQuery

An end-to-end data engineering project: synthetic apparel retail data (sales,
customers, products, stores) is generated in batches, landed in BigQuery, and
transformed into a dimensional model with **Apache Airflow** orchestrating and
**dbt** owning the SQL transformations.

This is a deliberate re-implementation of an existing Databricks Delta Live
Tables project on a different stack, using the *same* source data generator
and the *same* data-quality problems, to show the same data engineering
problem (CDC dimension upserts, referential integrity, dirty data) solved with
a different, very common toolchain (Airflow + dbt + a cloud warehouse instead
of Databricks DLT + Delta Lake).

## Architecture

```
                       ┌─────────────────────────┐
                       │   generator/*.py         │   pandas + Faker, adapted
                       │   (batch mode, was a     │   from a Databricks/Spark
                       │   Spark streaming job)   │   streaming generator
                       └────────────┬─────────────┘
                                    │ CSV per batch
                                    ▼
                       data/raw/{customers,products,stores,sales}/*.csv
                                    │
                                    │ scripts/load_to_bigquery.py (WRITE_APPEND)
                                    ▼
                 BigQuery: apparel_raw.{customers,products,stores,sales}_raw
                                    │
                                    │ dbt staging models (cast/clean, flag dq issues)
                                    ▼
                 BigQuery: apparel_analytics_staging.stg_*
                                    │
                                    │ dbt marts (incremental + merge, SCD Type 1)
                                    ▼
     BigQuery: apparel_analytics.{dim_customers, dim_products, dim_stores, fct_sales}
```

All of the above is orchestrated by one Airflow DAG: `dags/apparel_pipeline_dag.py`.

## Why this design

**Generator → batch, not streaming.** The original script (`generator/` is
adapted from it — see `data_generator.py` ) ran four infinite
loops inside a Spark cluster, writing to Delta every `BATCH_INTERVAL_S`
seconds. Airflow tasks are meant to run, finish, and exit — not loop forever —
so each `generate_*_stream` became a `generate_*_batch()` function that
produces exactly one batch and returns. The DAG's schedule (`every 15
minutes` by default) is what replaces the old `while True: ... sleep(30)`
cadence.

**Referential integrity across tasks, not just within one process.** The
original generator kept `existing_customers` / `existing_products` /
`existing_stores` in memory and refused to generate a sale until all three
had data. Since each Airflow task is a separate process, that check now
happens by querying BigQuery directly (`generator/state.py`), and the DAG
enforces the *ordering* with real task dependencies: `generate_sales` only
runs after all three dimension batches have been generated **and loaded** to
BigQuery.

**Same dirty data, on purpose.** Negative quantities (returns), negative
discounts (bad data), missing emails/managers, and CDC-style duplicate rows
with a new `last_update_time` are all still produced by the generator. The
dbt project is built to detect and handle them:

| Issue | Where it's handled |
|---|---|
| CDC updates (same ID, new data) | `dim_customers` / `dim_products` / `dim_stores` — incremental + `merge` on the business key, `QUALIFY ROW_NUMBER()` to collapse multiple updates in one batch |
| Missing email / manager | Flagged via `is_missing_email` / `is_missing_manager` columns in staging — not tested as failures, since it's valid data |
| Negative discount / price | Flagged via `has_invalid_discount` / `has_invalid_price`, and surfaced with a **warn-severity** `dbt_expectations` test so it's visible without failing the pipeline |
| Returns (negative quantity) | Flagged via `is_return` — not an error, a legitimate business event |
| Referential integrity | `relationships` tests in both `staging/schema.yml` and `marts/schema.yml` (fact → dimensions) |
| Event-time skew / late data | `fct_sales` intentionally uses the monotonic `transaction_id` as its incremental cursor instead of `event_time`, to avoid silently dropping late-arriving rows — see the comment in `fct_sales.sql` for why |

**dbt merge instead of manual PySpark merge.** The Databricks version of this
pattern hand-rolls the old/new split and `DeltaTable.merge()` call in a
parametrized notebook. Here, the exact same SCD Type 1 behavior is expressed
idiomatically with dbt's `incremental` materialization + `merge` strategy —
same outcome, less code, and it's the pattern most real dbt/BigQuery shops
actually use.

## Project layout

```
generator/                  batch data generator (was the Spark streaming script)
  config.py                 shared settings (counts, paths, GCP project/dataset)
  state.py                  loads existing IDs from BigQuery (referential integrity)
  generate_batch.py         the four generate_*_batch() functions + CLI
scripts/
  load_to_bigquery.py       appends the latest generated CSV into a BigQuery raw table
dags/
  apparel_pipeline_dag.py   the Airflow DAG
dbt/apparel_dbt/
  models/staging/           source defs + cleaning models (1:1 with raw tables)
  models/marts/             dim_customers, dim_products, dim_stores, fct_sales
  dbt_project.yml
  packages.yml              dbt_utils, dbt_expectations
  profiles_example.yml      copy to profiles.yml (or ~/.dbt/profiles.yml) and fill in
docker-compose.yml           local Airflow (LocalExecutor + Postgres)
requirements.txt
.env.example
```

## Setup

### 1. GCP / BigQuery

1. Create (or pick) a GCP project and enable the BigQuery API.
2. Create a service account with **BigQuery Data Editor** + **BigQuery Job
   User** roles, and download its JSON key.
3. Place the key at `keys/bq_service_account.json` (this path is git-ignored).
4. Copy `.env.example` to `.env` and fill in `GCP_PROJECT` (the raw dataset
   `apparel_raw` and the analytics dataset(s) are created automatically —
   the loader creates the raw dataset on first run, and dbt creates the
   `apparel_analytics` / `apparel_analytics_staging` datasets on first `dbt run`).

### 2. dbt profile

```bash
cp dbt/apparel_dbt/profiles_example.yml dbt/apparel_dbt/profiles.yml
# edit dbt/apparel_dbt/profiles.yml: set project + keyfile path
```

### 3. Run Airflow locally

```bash
docker compose up airflow-init   # one-time: migrate DB, create admin user (admin/admin)
docker compose up                # start postgres, webserver, scheduler
```

Open http://localhost:8080 (admin/admin), unpause `apparel_pipeline`, and
trigger a manual run. The first run bootstraps the initial customers/products/
stores population; every run after that produces incremental batches with a
mix of new rows and CDC-style updates.

### 4. Run pieces locally without Docker (optional, for development)

```bash
pip install -r requirements.txt
export PYTHONPATH=$(pwd)
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/keys/bq_service_account.json
export GCP_PROJECT=your-gcp-project

python -m generator.generate_batch customers
python -m generator.generate_batch products
python -m generator.generate_batch stores
python -m generator.generate_batch sales      # needs the three above loaded first

python -m scripts.load_to_bigquery customers
python -m scripts.load_to_bigquery products
python -m scripts.load_to_bigquery stores
python -m scripts.load_to_bigquery sales

cd dbt/apparel_dbt
dbt deps
dbt run
dbt test
```

Without any GCP credentials configured, the generator still runs standalone
(it falls back to a local JSON cache in `data/state/` for ID lookups) — handy
for a quick sanity check of the generation logic before wiring up BigQuery.

## Extending this project

- **Swap the local CSV landing zone for GCS**: point `generator/config.py` at
  a `gs://` bucket and use `GCSToBigQueryOperator` instead of the local-file
  `load_to_bigquery.py` script — more representative of a production landing
  zone.
- **Add `astronomer-cosmos`** to turn each dbt model into its own Airflow
  task (instead of one `BashOperator` per `dbt run`/`dbt test` invocation),
  for finer-grained retries and lineage in the Airflow UI.
- **Add Great Expectations or Soda** as an explicit data-quality gate between
  the raw load and the dbt run, instead of relying solely on dbt tests.
- **Surrogate keys**: the marts currently reuse the generator's natural
  integer IDs directly. If you want to mirror the Databricks project's
  surrogate-key pattern exactly, add `dbt_utils.generate_surrogate_key` in
  each dimension model and a matching lookup join in `fct_sales`.
