with source as (

    select * from {{ source('apparel_raw', 'products_raw') }}

),

cleaned as (

    select
        cast(product_id as int64)          as product_id,
        trim(name)                         as name,
        category,
        brand,
        cast(price as float64)             as price,
        cast(stock_quantity as int64)      as stock_quantity,
        size,
        color,
        description,
        cast(last_update_time as timestamp) as last_update_time,
        price < 0                          as has_invalid_price

    from source
    where product_id is not null

)

select * from cleaned
