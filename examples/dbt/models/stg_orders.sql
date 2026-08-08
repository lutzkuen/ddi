{{ config(
    materialized='external',
    plugin='delta_write',
    delta_path=env_var('DDI_LAKE') ~ '/stg_orders'
) }}

-- jaffle shop's stg_orders, unchanged. Nothing here knows that ddi exists.
with source as (

    select * from {{ source('bronze', 'raw_orders') }}

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

)

select * from renamed
