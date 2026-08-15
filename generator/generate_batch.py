"""
Batch-mode apparel data generator.

This is a direct adaptation of the original Databricks DLT `data_generator.py`
(the four `generate_*_stream` functions running forever inside a ThreadPoolExecutor,
writing to Delta via Spark). Airflow doesn't want a long-running infinite loop
inside a task - it wants a task that runs once, does its work, and exits - so
each stream became a "generate one batch" function that:

  1. Loads existing IDs (from BigQuery, so referential integrity is preserved
     against what's actually landed, same as the original `load_existing_ids`).
  2. On the very first run (no existing IDs), bootstraps an initial population.
  3. On every run, produces a batch of brand-new rows AND CDC-style updates to
     existing rows (same ID, new `last_update_time`) for the three dimension
     entities, and a batch of new sales transactions.
  4. Writes the batch to a timestamped CSV under data/raw/<entity>/, which the
     Airflow DAG then loads (append-only) into a BigQuery raw table.

All of the intentional "dirty data" behaviour from the original script is
preserved on purpose, because the downstream dbt project is built to detect
and handle exactly these issues:
  - sales.quantity can go negative (a return)
  - sales.discount_applied can go negative (bad data - should be flagged)
  - customers.email / stores.manager can be NULL
  - customer/product/store batches re-emit existing IDs with new attributes
    and a fresh last_update_time (CDC - the dbt marts apply this as SCD Type 1)
  - event_time / last_update_time are back-dated by up to LATENCY_MAX_S seconds
    to simulate out-of-order / late-arriving data
"""

import argparse
import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker

from generator.config import (
    CONFIG,
    RAW_CUSTOMERS_DIR,
    RAW_PRODUCTS_DIR,
    RAW_SALES_DIR,
    RAW_STORES_DIR,
    CATEGORIES,
    BRANDS,
    PAYMENT_METHODS,
    SIZES,
)
from generator.state import load_existing_ids

fake = Faker()


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_batch(df: pd.DataFrame, out_dir: str, entity: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{entity}_{_run_timestamp()}.csv")
    df.to_csv(path, index=False)
    print(f"[{entity}] wrote {len(df)} rows -> {path}")
    return path


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------
def generate_customers_batch() -> str:
    existing_ids = load_existing_ids("customers", "customers_raw", "customer_id")
    now = datetime.now(timezone.utc)
    rows = []

    if not existing_ids:
        print(f"[customers] bootstrapping {CONFIG['INITIAL_CUSTOMER_COUNT']} initial customers")
        for cid in range(1, CONFIG["INITIAL_CUSTOMER_COUNT"] + 1):
            rows.append(_customer_row(cid, now))
        existing_ids = list(range(1, CONFIG["INITIAL_CUSTOMER_COUNT"] + 1))
    else:
        next_id = max(existing_ids) + 1
        n = random.randint(1, CONFIG["CUSTOMER_BATCH_SIZE"])
        for _ in range(n):
            ts = now - timedelta(seconds=random.uniform(0, CONFIG["LATENCY_MAX_S"]))
            if existing_ids and random.random() < 0.3:
                cid = random.choice(existing_ids)  # CDC-style update to an existing customer
            else:
                cid = next_id
                next_id += 1
            rows.append(_customer_row(cid, ts))

    return _write_batch(pd.DataFrame(rows), RAW_CUSTOMERS_DIR, "customers")


def _customer_row(customer_id: int, ts: datetime) -> dict:
    return {
        "customer_id": customer_id,
        "name": fake.name(),
        "email": fake.email() if random.random() >= 0.02 else None,  # ~2% missing on purpose
        "address": fake.address().replace("\n", ", "),
        "join_date": (ts - timedelta(days=random.randint(1, 365))).isoformat(),
        "loyalty_points": random.randint(0, 1000),
        "phone_number": fake.phone_number(),
        "age": random.randint(18, 70),
        "gender": random.choice(["Male", "Female", "Other"]),
        "last_update_time": ts.isoformat(),
    }


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------
def generate_products_batch() -> str:
    existing_ids = load_existing_ids("products", "products_raw", "product_id")
    now = datetime.now(timezone.utc)
    rows = []

    if not existing_ids:
        print(f"[products] bootstrapping {CONFIG['INITIAL_PRODUCT_COUNT']} initial products")
        for pid in range(1, CONFIG["INITIAL_PRODUCT_COUNT"] + 1):
            rows.append(_product_row(pid, now))
        existing_ids = list(range(1, CONFIG["INITIAL_PRODUCT_COUNT"] + 1))
    else:
        next_id = max(existing_ids) + 1
        n = random.randint(1, CONFIG["PRODUCT_BATCH_SIZE"])
        for _ in range(n):
            ts = now - timedelta(seconds=random.uniform(0, CONFIG["LATENCY_MAX_S"]))
            if existing_ids and random.random() < 0.3:
                pid = random.choice(existing_ids)  # CDC-style update (e.g. price/stock change)
            else:
                pid = next_id
                next_id += 1
            rows.append(_product_row(pid, ts))

    return _write_batch(pd.DataFrame(rows), RAW_PRODUCTS_DIR, "products")


def _product_row(product_id: int, ts: datetime) -> dict:
    color = fake.color_name()
    size = random.choice(SIZES)
    name = f"{fake.word().capitalize()} {color} {size}"
    return {
        "product_id": product_id,
        "name": name,
        "category": random.choice(CATEGORIES),
        "brand": random.choice(BRANDS),
        "price": round(random.uniform(10.0, 150.0), 2),
        "stock_quantity": random.randint(0, 200),
        "size": size,
        "color": color,
        "description": fake.sentence(nb_words=8),
        "last_update_time": ts.isoformat(),
    }


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------
def generate_stores_batch() -> str:
    existing_ids = load_existing_ids("stores", "stores_raw", "store_id")
    now = datetime.now(timezone.utc)
    rows = []

    if not existing_ids:
        print(f"[stores] bootstrapping {CONFIG['INITIAL_STORE_COUNT']} initial stores")
        for sid in range(1, CONFIG["INITIAL_STORE_COUNT"] + 1):
            rows.append(_store_row(sid, now, status="Open"))
        existing_ids = list(range(1, CONFIG["INITIAL_STORE_COUNT"] + 1))
    else:
        next_id = max(existing_ids) + 1
        n = random.randint(0, CONFIG["STORE_BATCH_SIZE"])  # 0 allowed: not every run opens a store
        for _ in range(n):
            ts = now - timedelta(seconds=random.uniform(0, CONFIG["LATENCY_MAX_S"]))
            if existing_ids and random.random() < 0.5:
                sid = random.choice(existing_ids)  # CDC-style update (status/manager change)
            else:
                sid = next_id
                next_id += 1
            rows.append(_store_row(sid, ts, status=random.choice(["Open", "Under Renovation"])))

    if not rows:
        print("[stores] no new/updated stores this batch - skipping file write")
        return ""

    return _write_batch(pd.DataFrame(rows), RAW_STORES_DIR, "stores")


def _store_row(store_id: int, ts: datetime, status: str) -> dict:
    return {
        "store_id": store_id,
        "name": f"Store {store_id} - {fake.city()}",
        "address": fake.address().replace("\n", ", "),
        "manager": fake.name() if random.random() >= 0.02 else None,  # occasionally vacant
        "open_date": (ts - timedelta(days=random.randint(365, 1825))).isoformat(),
        "status": status,
        "phone_number": fake.phone_number(),
        "last_update_time": ts.isoformat(),
    }


# ---------------------------------------------------------------------------
# Sales (fact stream - depends on the three dimensions above already existing)
# ---------------------------------------------------------------------------
def generate_sales_batch() -> str:
    existing_customers = load_existing_ids("customers", "customers_raw", "customer_id")
    existing_products = load_existing_ids("products", "products_raw", "product_id")
    existing_stores = load_existing_ids("stores", "stores_raw", "store_id")

    if not (existing_customers and existing_products and existing_stores):
        raise RuntimeError(
            "Sales batch requires customers, products, and stores to already be loaded. "
            "Make sure the dimension tasks ran (and loaded to BigQuery) before this task."
        )

    max_id = load_existing_ids("sales", "sales_raw", "transaction_id")
    transaction_id_counter = (max(max_id) + 1) if max_id else 1

    now = datetime.now(timezone.utc)
    rows = []
    for _ in range(CONFIG["TRANSACTIONS_PER_BATCH"]):
        ts = now - timedelta(seconds=random.uniform(0, CONFIG["LATENCY_MAX_S"]))
        customer_id = random.choice(existing_customers)
        product_id = random.choice(existing_products)
        store_id = random.choice(existing_stores)

        quantity = random.randint(1, 5)
        if random.random() < 0.05:
            quantity = -quantity  # ~5% returns, on purpose

        unit_price = round(random.uniform(10.0, 100.0), 2)
        total_amount = round(quantity * unit_price, 2)
        discount = round(random.uniform(0, 20), 2)
        if random.random() < 0.02:
            discount = -discount  # ~2% invalid discount, on purpose

        rows.append(
            {
                "transaction_id": transaction_id_counter,
                "store_id": store_id,
                "event_time": ts.isoformat(),
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "payment_method": random.choice(PAYMENT_METHODS),
                "discount_applied": discount,
                "tax_amount": round(total_amount * 0.08, 2),
            }
        )
        transaction_id_counter += 1

    return _write_batch(pd.DataFrame(rows), RAW_SALES_DIR, "sales")


# ---------------------------------------------------------------------------
# CLI entrypoint - used both for local testing and as the Airflow task command
# ---------------------------------------------------------------------------
ENTITY_FUNCS = {
    "customers": generate_customers_batch,
    "products": generate_products_batch,
    "stores": generate_stores_batch,
    "sales": generate_sales_batch,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate one batch of synthetic apparel data.")
    parser.add_argument("entity", choices=ENTITY_FUNCS.keys())
    args = parser.parse_args()

    output_path = ENTITY_FUNCS[args.entity]()
    print(f"done: {output_path or '(no file written)'}")
