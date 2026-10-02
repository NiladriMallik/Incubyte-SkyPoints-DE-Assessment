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
    r.batch_id
from {{ ref('stg_redemptions') }} r
left join {{ ref('members') }} m
    on r.member_id = m.member_id