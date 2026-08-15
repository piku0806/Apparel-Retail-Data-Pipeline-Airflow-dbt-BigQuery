-- Sales are already one row per transaction_id in the raw log (no CDC here -
-- a booking never mutates once created), so staging just casts types and
-- flags the intentional data-quality issues rather than filtering them out:
-- a negative quantity is a legitimate return, and a negative discount is bad
-- data that downstream consumers should be able to see and exclude if needed.

with source as (

    select * from {{ source('apparel_raw', 'sales_raw') }}

),

cleaned as (

    select
        cast(transaction_id as int64)        as transaction_id,
        cast(store_id as int64)              as store_id,
        cast(event_time as timestamp)        as event_time,
        cast(customer_id as int64)           as customer_id,
        cast(product_id as int64)            as product_id,
        cast(quantity as int64)              as quantity,
        cast(unit_price as float64)          as unit_price,
        cast(total_amount as float64)        as total_amount,
        payment_method,
        cast(discount_applied as float64)    as discount_applied,
        cast(tax_amount as float64)          as tax_amount,

        cast(quantity as int64) < 0          as is_return,
        cast(discount_applied as float64) < 0 as has_invalid_discount

    from source
    where transaction_id is not null

)

select * from cleaned
