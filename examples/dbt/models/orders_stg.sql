{{ config(
    materialized='external',
    plugin='delta_write',
    delta_path=env_var('DDI_LAKE') ~ '/orders_stg'
) }}

-- Parse the bronze payload into typed columns. Nothing here knows ddi exists; this
-- is the SQL a warehouse would run for the nightly rebuild.
--
-- json_extract_string is DuckDB's spelling, so this file is what the warehouse runs.
-- ddi registers it along with Trino/Starburst's json_extract_scalar and Spark's
-- get_json_object, all with identical behaviour, so the same model streams unchanged
-- whichever engine the batch side happens to be.
with source as (

    select * from {{ source('bronze', 'orders_raw') }}

),

parsed as (

    select
        order_id,
        cast(json_extract_string(data, '$.customer_id') as bigint) as customer_id,
        cast(json_extract_string(data, '$.amount')      as bigint) as amount,
        json_extract_string(data, '$.status')                      as status,
        _timestamp

    from source

)

select * from parsed
