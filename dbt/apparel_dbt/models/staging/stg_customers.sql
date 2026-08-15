-- Light cleaning/casting pass over the raw customer CDC log.
-- No dedup here on purpose: dim_customers (incremental + merge) is what
-- collapses this into "one current row per customer_id".

with source as (

    select * from {{ source('apparel_raw', 'customers_raw') }}

),

cleaned as (

    select
        cast(customer_id as int64)                     as customer_id,
        trim(name)                                      as name,
        nullif(trim(email), '')                         as email,
        address,
        cast(join_date as timestamp)                     as join_date,
        cast(loyalty_points as int64)                    as loyalty_points,
        phone_number,
        cast(age as int64)                               as age,
        gender,
        cast(last_update_time as timestamp)              as last_update_time,
        email is null                                    as is_missing_email

    from source
    where customer_id is not null

)

select * from cleaned
