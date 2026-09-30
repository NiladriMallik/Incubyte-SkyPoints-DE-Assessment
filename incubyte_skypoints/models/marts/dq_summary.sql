with enriched as (
    select * from {{ ref('int_member_enriched') }}
)

select
    country_code,
    batch_id,
    source_file_name,
    count(*)                                                    as total_rows,
    count_if(dq_status = 'OK')                                  as ok_rows,
    count_if(dq_status = 'REVIEW')                              as review_rows,
    round(100 * count_if(dq_status = 'REVIEW') / count(*), 2)   as review_pct,
    count_if(is_enrollment_date_invalid)                        as invalid_enrollment_dates,
    count_if(is_flight_date_invalid)                            as invalid_flight_dates,
    count_if(is_dob_missing_or_invalid)                         as dob_missing_or_invalid,
    count_if(is_flight_before_enrollment)                       as flight_before_enrollment,
    max(load_timestamp)                                         as last_loaded_at
from enriched
group by country_code, batch_id, source_file_name