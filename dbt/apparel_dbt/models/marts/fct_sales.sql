-- Sales fact table. Unlike the dimensions, sales rows never mutate once
-- created, so instead of a CDC timestamp cursor we use transaction_id -
-- a monotonically increasing counter the generator resumes from
-- max(transaction_id) on every run. That sidesteps the event_time skew
-- (event_time is intentionally backdated by up to LATENCY_MAX_S seconds to
-- simulate late-arriving data - a good watermarking exercise, but a risky
-- incremental cursor: a late row's event_time could fall *before* the max
-- event_time already merged into this table and get silently skipped).
--
-- Dirty data (is_return, has_invalid_discount) is kept and flagged, not
-- dropped - same design intent as the original generator's docstring.

{{ config(unique_key='transaction_id') }}

with source as (

    select * from {{ ref('stg_sales') }}

    {% if is_incremental() %}
    where transaction_id > (select coalesce(max(transaction_id), 0) from {{ this }})
    {% endif %}

)

select
    transaction_id,
    store_id,
    customer_id,
    product_id,
    event_time,
    quantity,
    unit_price,
    total_amount,
    discount_applied,
    tax_amount,
    payment_method,
    is_return,
    has_invalid_discount,
    current_timestamp() as dbt_loaded_at

from source
