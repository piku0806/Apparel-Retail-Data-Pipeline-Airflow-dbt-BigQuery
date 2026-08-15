-- SCD Type 1 store dimension (status/manager changes overwrite in place).
-- See dim_customers.sql for the full explanation of the pattern.

{{ config(unique_key='store_id') }}

with source as (

    select * from {{ ref('stg_stores') }}

    {% if is_incremental() %}
    where last_update_time > (select coalesce(max(last_update_time), timestamp('1900-01-01')) from {{ this }})
    {% endif %}

),

deduped as (

    select *
    from source
    qualify row_number() over (partition by store_id order by last_update_time desc) = 1

)

select
    d.store_id,
    d.name,
    d.address,
    d.manager,
    d.is_missing_manager,
    d.open_date,
    d.status,
    d.phone_number,

    {% if is_incremental() %}
    coalesce(existing.create_date, d.last_update_time) as create_date,
    {% else %}
    d.last_update_time as create_date,
    {% endif %}

    d.last_update_time as update_date,
    current_timestamp() as dbt_loaded_at

from deduped d
{% if is_incremental() %}
left join {{ this }} existing on existing.store_id = d.store_id
{% endif %}
