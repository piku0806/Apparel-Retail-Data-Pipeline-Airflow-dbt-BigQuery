with source as (

    select * from {{ source('apparel_raw', 'stores_raw') }}

),

cleaned as (

    select
        cast(store_id as int64)             as store_id,
        name,
        address,
        nullif(trim(manager), '')           as manager,
        cast(open_date as timestamp)        as open_date,
        status,
        phone_number,
        cast(last_update_time as timestamp) as last_update_time,
        manager is null                     as is_missing_manager

    from source
    where store_id is not null

)

select * from cleaned
