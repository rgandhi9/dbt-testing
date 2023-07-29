{{ config(materialized="view") }}
select
    timestamp_seconds(cast(timestamp as int)) as transaction_timestamp,
    date(timestamp_seconds(cast(timestamp as int))) as transaction_date,
    transaction_id,
    pair,
    transaction_rate,
    transaction_amount,
    order_side,
    taker_id,
    maker_id
from {{ source("coincheck_btc_jpy", "raw_data") }}
