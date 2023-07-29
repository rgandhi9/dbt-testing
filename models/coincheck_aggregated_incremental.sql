{{
    config(
        materialized="incremental",
        partition_by={"field": "transaction_date", "data_type": "date"},
        incremental_strategy="insert_overwrite",
    )
}}
select *
from {{ref('coincheck_aggregated')}}
{% if is_incremental() %}
    where transaction_date >= date_sub(date(_dbt_max_partition), interval 1 day)
{% endif %}
