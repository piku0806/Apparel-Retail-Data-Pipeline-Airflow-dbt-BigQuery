-- SCD Type 1 customer dimension, built with dbt's incremental + merge
-- materialization. This is the dbt-idiomatic version of the manual
-- "old vs new split + surrogate key + DeltaTable.merge()" pattern used in
-- the companion Databricks project - same problem (CDC dimension upsert),
-- different engine.

{{ config(unique_key='customer_id') }}

with source as (

    select * from {{ ref('stg_customers') }}

    {% if is_incremental() %}
    where last_update_time > (select coalesce(max(last_update_time), timestamp('1900-01-01')) from {{ this }})
    {% endif %}

),

-- A single incremental batch could in theory contain more than one update
-- for the same customer_id; keep only the latest.
deduped as (

    select *
    from source
    qualify row_number() over (partition by customer_id order by last_update_time desc) = 1

)

select
    d.customer_id,
    d.name,
    d.email,
    d.address,
    d.join_date,
    d.loyalty_points,
    d.phone_number,
    d.age,
    d.gender,
    d.is_missing_email,

    {% if is_incremental() %}
    coalesce(existing.create_date, d.last_update_time) as create_date,
    {% else %}
    d.last_update_time as create_date,
    {% endif %}

    d.last_update_time as update_date,
    current_timestamp() as dbt_loaded_at

from deduped d
{% if is_incremental() %}
left join {{ this }} existing on existing.customer_id = d.customer_id
{% endif %}
