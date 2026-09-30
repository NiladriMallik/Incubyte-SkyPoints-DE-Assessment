select
    member_key,
    member_id,
    member_name,
    tier_code,
    tier_name,
    date_of_birth,
    age_at_enrollment,
    current_age,
    enrollment_date,
    flight_date,
    days_enrollment_to_flight,
    has_flown,
    is_enrollment_date_invalid,
    is_flight_date_invalid,
    is_dob_missing_or_invalid,
    is_flight_before_enrollment,
    dq_status,
    dq_issues,
    source_file_name,
    batch_id,
    load_timestamp
from {{ ref('int_member_enriched') }}
where country_code = 'AUS'