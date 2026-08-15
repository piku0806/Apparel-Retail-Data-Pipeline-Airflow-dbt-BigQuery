"""
Apparel Retail Pipeline (Airflow + dbt + BigQuery)

This DAG replaces the "always-on Spark cluster running four infinite threads"
model from the original Databricks DLT generator with a micro-batch model:
every DAG run produces one new batch of synthetic data for each entity, loads
it into BigQuery raw tables, and then runs dbt to clean it up and merge it
into dimension/fact tables.

Dependency shape (mirrors the original generator's own dependency rule -
"sales waits until customers/products/stores have data"):

    generate_customers -> load_customers   \
    generate_products  -> load_products     >-> generate_sales -> load_sales -\
    generate_stores    -> load_stores      /                                   \
                                                                                  >-> dbt_deps -> dbt_run_staging -> dbt_test_staging -> dbt_run_marts -> dbt_test_marts
    (load_customers, load_products, load_stores also feed dbt directly) ------/

Schedule: every 15 minutes, standing in for the original BATCH_INTERVAL_S=30s
micro-batches (an Airflow task has too much overhead to run every 30s -
15 minutes is a reasonable "near-real-time batch" cadence for a portfolio demo;
tighten it if you want to demonstrate higher frequency).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

DBT_PROJECT_DIR = "/opt/airflow/dbt/apparel_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt/apparel_dbt"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="apparel_pipeline",
    description="Generate synthetic apparel retail data, land it in BigQuery, transform with dbt.",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["apparel", "bigquery", "dbt"],
) as dag:

    # -------------------------------------------------------------------
    # 1. Generate + load the three dimension streams (independent of each other)
    # -------------------------------------------------------------------
    with TaskGroup("dimensions") as dimensions_group:
        dim_tasks = {}
        for entity in ["customers", "products", "stores"]:

            def _generate(entity=entity):
                from generator.generate_batch import ENTITY_FUNCS
                ENTITY_FUNCS[entity]()

            def _load(entity=entity):
                from scripts.load_to_bigquery import load_entity
                load_entity(entity)

            generate_task = PythonOperator(
                task_id=f"generate_{entity}",
                python_callable=_generate,
            )
            load_task = PythonOperator(
                task_id=f"load_{entity}_to_bq",
                python_callable=_load,
            )
            generate_task >> load_task
            dim_tasks[entity] = load_task

    # -------------------------------------------------------------------
    # 2. Generate + load sales, once all three dimensions have landed in BigQuery
    #    (this is what preserves referential integrity, same as the original
    #    generator's `load_existing_ids` check before emitting a transaction)
    # -------------------------------------------------------------------
    def _generate_sales():
        from generator.generate_batch import generate_sales_batch
        generate_sales_batch()

    def _load_sales():
        from scripts.load_to_bigquery import load_entity
        load_entity("sales")

    generate_sales = PythonOperator(task_id="generate_sales", python_callable=_generate_sales)
    load_sales = PythonOperator(task_id="load_sales_to_bq", python_callable=_load_sales)

    list(dim_tasks.values()) >> generate_sales >> load_sales

    # -------------------------------------------------------------------
    # 3. dbt: clean/stage raw data, then merge into dimensions and the fact table
    # -------------------------------------------------------------------
    dbt_env = {"DBT_PROFILES_DIR": DBT_PROFILES_DIR}

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps --project-dir {DBT_PROJECT_DIR}",
        env=dbt_env,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --select staging.*",
        env=dbt_env,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --select staging.*",
        env=dbt_env,
    )

    # Dimensions must be merged before the fact table (fact relationship
    # tests validate against them), so marts run dims first, then fct_sales.
    dbt_run_dims = BashOperator(
        task_id="dbt_run_dims",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--select marts.dim_customers marts.dim_products marts.dim_stores"
        ),
        env=dbt_env,
    )

    dbt_run_fact = BashOperator(
        task_id="dbt_run_fact",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --select marts.fct_sales",
        env=dbt_env,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --select marts.*",
        env=dbt_env,
    )

    [dimensions_group, load_sales] >> dbt_deps >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_dims >> dbt_run_fact >> dbt_test_marts
