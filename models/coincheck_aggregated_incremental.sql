{% set partitions_to_replace = [
    "current_date()",
    "date_sub(current_date, interval 1 day)",
] %}
{{
    config(
        materialized="incremental",
        partition_by={"field": "transaction_date", "data_type": "date"},
        incremental_strategy="insert_overwrite",
        partitions=partitions_to_replace,
    )
}}
select *
from {{ ref("coincheck_aggregated") }}
{% if is_incremental() %}
    where transaction_date in ({{ partitions_to_replace | join(",") }})
{% endif %}
