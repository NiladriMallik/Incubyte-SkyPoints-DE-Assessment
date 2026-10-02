{{
    config(
        materialized='incremental',
        unique_key='member_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

select
    member_key,
    member_id,
    member_name,
    country_code,
    previous_country_code,
    country_changed,
    state,
    agent_name,
    is_active,
    tier_code,
    tier_name,
    date_of_birth,
    age_at_enrollment,
    current_age,
    enrollment_date,
    flight_date,
    days_enrollment_to_flight,
    days_since_last_flight,
    has_flown,
    stale_member,
    dq_status,
    dq_issues,
    source_file_name,
    batch_id,
    load_timestamp,
    _updated_at
from {{ ref('int_member_enriched') }}
{% if is_incremental() %}
  where _updated_at > (
    select coalesce(max(_updated_at), '1900-01-01'::timestamp_ntz) from {{ this }}
  )
{% endif %}