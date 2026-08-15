"""
Shared configuration for the apparel data generator.

This mirrors the CONFIG dict from the original Databricks/DLT `data_generator.py`,
but is reused here in a *batch* context: each Airflow task run calls the generator
once and produces "one batch" of new/updated rows, instead of looping forever
inside a long-running Spark cluster.
"""

import os

# --- GCP / BigQuery settings -------------------------------------------------
GCP_PROJECT = os.environ.get("GCP_PROJECT", "your-gcp-project")
BQ_DATASET_RAW = os.environ.get("BQ_DATASET_RAW", "apparel_raw")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# --- Local landing zone (simulates the "raw"/bronze folder from the DLT version)
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_CUSTOMERS_DIR = os.path.join(DATA_DIR, "raw", "customers")
RAW_PRODUCTS_DIR = os.path.join(DATA_DIR, "raw", "products")
RAW_STORES_DIR = os.path.join(DATA_DIR, "raw", "stores")
RAW_SALES_DIR = os.path.join(DATA_DIR, "raw", "sales")
STATE_DIR = os.path.join(DATA_DIR, "state")

# --- Generation volumes (same names/values as the original generator) -------
CONFIG = {
    # Initial bootstrap counts (first run only, when the BigQuery table is empty)
    "INITIAL_STORE_COUNT": 5,
    "INITIAL_CUSTOMER_COUNT": 50,
    "INITIAL_PRODUCT_COUNT": 20,

    # Per-batch (per DAG run) generation counts
    "TRANSACTIONS_PER_BATCH": 500,
    "CUSTOMER_BATCH_SIZE": 30,
    "PRODUCT_BATCH_SIZE": 20,
    "STORE_BATCH_SIZE": 10,

    # Event-time skew, in seconds - simulates late-arriving data
    "LATENCY_MAX_S": 60,
}

# --- Reference data lists (unchanged from the original generator) -----------
PRODUCT_TYPES = ["T-Shirt", "Jeans", "Jacket", "Dress", "Shoes", "Hat", "Scarf", "Sweater", "Pants", "Shirt"]
SIZES = ["XS", "S", "M", "L", "XL"]
CATEGORIES = ["Casual Wear", "Formal Wear", "Sportswear", "Accessories", "Footwear"]
BRANDS = [
    "Urban Threads", "Peak Performance", "Vivid Apparel", "Heritage Denim", "Metro Style",
    "Canvas Collective", "Elemental Gear", "Luxe Layers", "Stride & Co.", "Echo Streetwear",
    "Summit Outfitters", "Blue Horizon", "Modern Muse", "Threadsmiths", "Crestline",
    "Northbridge", "Vogue Venture", "Atlas Attire", "Pulse Activewear", "Signature Stitch",
]
PAYMENT_METHODS = ["Cash", "Credit Card", "Debit Card", "Mobile Pay", "Gift Card"]
