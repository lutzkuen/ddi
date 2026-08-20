{{ config(materialized='view') }}

-- The live dashboard payload, and nothing more than ordinary dbt SQL.
--
-- Over the whole table this is the running total per status, which is what a browser
-- reads once as its baseline. Over a single committed batch — which is what ddi runs it
-- against, in memory, right after that batch commits — it is the delta to add to it.
-- The same SQL means both because sum and count combine across batches, and that is
-- exactly why only aggregates that do are allowed here.
--
-- Nothing about this file is ddi-specific: `dbt run` builds it, `dbt test` tests it, and
-- an analyst can see the whole live-dashboard logic without reading any Rust.
select
    status,
    count(*)    as orders_delta,
    sum(amount) as amount_delta

from {{ ref('orders_stg') }}

group by status
