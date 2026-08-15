"""
State helpers for the batch generator.

The original Databricks generator called `load_existing_ids()` against a Delta
table on every stream startup to preserve referential integrity (it only ever
generates sales for customer/product/store IDs that already exist downstream).

Here, "downstream" is a BigQuery raw table instead of a Delta table, so we do
the same lookup against BigQuery. If the table doesn't exist yet (first-ever
run) or BigQuery credentials aren't configured (e.g. local dry-run without
GCP), we fall back to a local JSON state file so the generator still works
standalone.
"""

import json
import os
from typing import List

from generator.config import GCP_PROJECT, BQ_DATASET_RAW, STATE_DIR


def _state_file(entity: str) -> str:
    os.makedirs(STATE_DIR, exist_ok=True)
    return os.path.join(STATE_DIR, f"{entity}_ids.json")


def load_existing_ids(entity: str, table: str, id_column: str) -> List[int]:
    """Load distinct existing IDs for `entity` from BigQuery.

    Falls back to a local JSON cache (data/state/<entity>_ids.json) if:
      * the BigQuery table doesn't exist yet (first run), or
      * BigQuery isn't reachable (e.g. running the generator locally without
        GOOGLE_APPLICATION_CREDENTIALS set, for a quick dry-run).
    """
    try:
        from google.cloud import bigquery
        from google.cloud.exceptions import NotFound

        client = bigquery.Client(project=GCP_PROJECT)
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET_RAW}.{table}"
        query = f"SELECT DISTINCT {id_column} FROM `{table_ref}`"
        try:
            rows = client.query(query).result()
            ids = sorted({row[id_column] for row in rows})
            print(f"[state] Loaded {len(ids)} existing {entity} IDs from BigQuery ({table_ref}).")
            _write_local_cache(entity, ids)
            return ids
        except NotFound:
            print(f"[state] {table_ref} does not exist yet - starting fresh for {entity}.")
            return _read_local_cache(entity)
    except Exception as exc:  # noqa: BLE001 - broad on purpose for local dry-runs
        print(f"[state] Could not query BigQuery for {entity} ids ({exc}). Falling back to local cache.")
        return _read_local_cache(entity)


def _read_local_cache(entity: str) -> List[int]:
    path = _state_file(entity)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return []


def _write_local_cache(entity: str, ids: List[int]) -> None:
    path = _state_file(entity)
    with open(path, "w") as f:
        json.dump(ids, f)


def update_local_cache(entity: str, new_ids: List[int]) -> List[int]:
    """Merge `new_ids` into the local cache for `entity` and persist it.

    Used by scripts/load_to_bigquery.py as an offline fallback (when the
    google-cloud-bigquery package/credentials aren't available) so the
    generator -> load -> generate-sales chain can still be exercised
    end-to-end without GCP, e.g. for a quick local dry-run.
    """
    existing = set(_read_local_cache(entity))
    merged = sorted(existing | set(new_ids))
    _write_local_cache(entity, merged)
    return merged
