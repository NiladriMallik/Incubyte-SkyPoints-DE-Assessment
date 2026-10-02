with source as (
    select * from {{ source('raw', 'redemption_feed') }}
),

flattened as(

    select
        trim(s.raw_payload:member_id::string)           as member_id,
        s.raw_payload:feed_date::string                 as feed_date_raw,
        r.value:txn_id::string                          as txn_id,
        r.value:txn_date::string                        as txn_date_raw,
        trim(r.value:partner::string)                   as partner,
        r.value:miles_redeemed::number                  as miles_redeemed,
        upper(trim(r.value:status::string))             as status,
        s.source_file_name,
        s.batch_id,
        s.load_timestamp
    from source s,
        lateral flatten(input => s.raw_payload:redemptions) r
),

typed as (
    select
        *,
        try_to_date(feed_date_raw, 'YYYYMMDD') as feed_date,
        try_to_date(txn_date_raw, 'YYYYMMDD') as txn_date
    from flattened
)

select
    *
from typed
qualify row_number() over (
    partition by txn_id
    order by feed_date desc nulls last, load_timestamp desc
) = 1