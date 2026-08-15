-- SCD Type 1 product dimension (price/stock/description changes overwrite
-- in place). See dim_customers.sql for the full explanation of the pattern.

{{ config(unique_key='product_id') }}

with source as (

    select * from {{ ref('stg_products') }}

    {% if is_incremental() %}
    where last_update_time > (select coalesce(max(last_update_time), timestamp('1900-01-01')) from {{ this }})
    {% endif %}

),

deduped as (

    select *
    from source
    qualify row_number() over (partition by product_id order by last_update_time desc) = 1

)

select
    d.product_id,
    d.name,
    d.category,
    d.brand,
    d.price,
    d.stock_quantity,
    d.size,
    d.color,
    d.description,
    d.has_invalid_price,

    {% if is_incremental() %}
    coalesce(existing.create_date, d.last_update_time) as create_date,
    {% else %}
    d.last_update_time as create_date,
    {% endif %}

    d.last_update_time as update_date,
    current_timestamp() as dbt_loaded_at

from deduped d
{% if is_incremental() %}
left join {{ this }} existing on existing.product_id = d.product_id
{% endif %}
