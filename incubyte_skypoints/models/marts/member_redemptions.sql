{{
    config(
        materialized='incremental',
        unique_id='txn_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

select
    r.txn_id,
    r.txn_date,
    r.partner,
    r.miles_redeemed,
    r.status,
    r.feed_date,
    r.member_id,
    m.member_name,
    m.country_code,
    m.tier_name,
    m.is_active,
    m.stale_member,
    (m.member_id is null) as is_orphan_txn,
    r.source_file_name,
    r.batch_id,
    r._updated_at
from {{ ref('stg_redemptions') }} r
left join {{ ref('members') }} m
on r.member_id = m.member_id
{% if is_incremental() %}
where r._updated_at > (
    select coalesce(max(_updated_at), '1900-01-01'::timestamp_ntz) from {{ this }}
)
{% endif %}