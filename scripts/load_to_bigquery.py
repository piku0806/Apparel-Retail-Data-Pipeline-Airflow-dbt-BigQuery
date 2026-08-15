"""
Loads the most recently generated CSV batch for one entity into its BigQuery
raw table (append-only - this is intentionally the "bronze" layer; dbt staging
models are what deduplicate / apply CDC on top of it).

Usage (also how Airflow calls it):
    python -m scripts.load_to_bigquery customers
    python -m scripts.load_to_bigquery products
    python -m scripts.load_to_bigquery stores
    python -m scripts.load_to_bigquery sales

Design notes:
  * Raw tables are named `<entity>_raw` in the BQ_DATASET_RAW dataset, matching
    what generator/state.py queries when it looks up existing IDs.
  * Load is WRITE_APPEND with schema autodetect + `allow_quoted_newlines`, so
    the same script works whether the table already exists or not (BigQuery
    creates it from the CSV header on the first run).
  * If a batch produced no file (e.g. a "0 new stores this run" batch), the
    task is a no-op success rather than a failure.
"""

import argparse
import glob
import os
import sys

from generator.config import (
    GCP_PROJECT,
    BQ_DATASET_RAW,
    BQ_LOCATION,
    RAW_CUSTOMERS_DIR,
    RAW_PRODUCTS_DIR,
    RAW_STORES_DIR,
    RAW_SALES_DIR,
)

ENTITY_DIRS = {
    "customers": (RAW_CUSTOMERS_DIR, "customers_raw"),
    "products": (RAW_PRODUCTS_DIR, "products_raw"),
    "stores": (RAW_STORES_DIR, "stores_raw"),
    "sales": (RAW_SALES_DIR, "sales_raw"),
}

ID_COLUMNS = {
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
    "sales": "transaction_id",
}


def _latest_csv(directory: str) -> str | None:
    if not os.path.isdir(directory):
        return None
    files = sorted(glob.glob(os.path.join(directory, "*.csv")))
    return files[-1] if files else None


def _load_offline_fallback(entity: str, csv_path: str) -> None:
    """Used only when google-cloud-bigquery isn't installed at all (e.g. a
    quick local dry-run without the Docker/Airflow setup). Records the batch's
    IDs into the local state cache so generator.state.load_existing_ids can
    still resolve referential integrity for the next entity in the chain
    (most importantly: dimensions -> sales). This is NOT a substitute for a
    real BigQuery load - nothing lands in a warehouse in this mode.
    """
    import pandas as pd

    id_col = ID_COLUMNS[entity]
    df = pd.read_csv(csv_path)
    ids = sorted(df[id_col].dropna().astype(int).unique().tolist())

    from generator.state import update_local_cache
    update_local_cache(entity, ids)

    print(f"[load:{entity}] google-cloud-bigquery not installed - recorded {len(ids)} id(s) "
          f"to the local state cache instead of BigQuery (offline dry-run mode).")


def load_entity(entity: str) -> None:
    directory, table_name = ENTITY_DIRS[entity]
    csv_path = _latest_csv(directory)

    if not csv_path:
        print(f"[load:{entity}] no CSV found in {directory} - nothing to load, skipping.")
        return

    try:
        from google.cloud import bigquery
    except ImportError:
        _load_offline_fallback(entity, csv_path)
        return

    client = bigquery.Client(project=GCP_PROJECT)
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT, BQ_DATASET_RAW)

    # Make sure the raw dataset exists (idempotent).
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
        print(f"[load:{entity}] created dataset {GCP_PROJECT}.{BQ_DATASET_RAW}")

    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        allow_quoted_newlines=True,
    )

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config, location=BQ_LOCATION)
    job.result()  # wait for completion, raises on failure

    print(f"[load:{entity}] loaded {csv_path} -> {GCP_PROJECT}.{BQ_DATASET_RAW}.{table_name} "
          f"({job.output_rows} rows).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load the latest generated batch for an entity into BigQuery.")
    parser.add_argument("entity", choices=ENTITY_DIRS.keys())
    args = parser.parse_args()

    try:
        load_entity(args.entity)
    except Exception as exc:  # noqa: BLE001
        print(f"[load:{args.entity}] FAILED: {exc}", file=sys.stderr)
        raise
