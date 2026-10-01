with base as (
    select * from {{ ref('stg_member_feed') }}
),

derived as(
    select
        {{ dbt_utils.generate_surrogate_key(['country_code', 'member_id']) }} as member_key,
        country_code,
        member_id,
        member_name,
        tier_code,
        case tier_code
            when 'PLT' then 'Platinum'
            when 'GLD' then 'Gold'
            when 'SLV' then 'Silver'
        end as tier_name,
        coalesce(member_type, 'UNKNOWN') as member_type,
        date_of_birth,
        enrollment_date,
        flight_date,

        -- age in completed years (datediff('year') alone only counts calendar boundaries)
        datediff('year', date_of_birth, enrollment_date)
        - iff(
            dateadd('year', datediff('year', date_of_birth, enrollment_date), date_of_birth) > enrollment_date, 1, 0
            ) as age_at_enrollment,

        datediff('year', date_of_birth, current_date())
        - iff(
            dateadd('year', datediff('year', date_of_birth, current_date()), date_of_birth) > current_date(), 1, 0
        ) as current_age,

        datediff('day', enrollment_date, flight_date) as days_enrollment_to_flight,

        datediff('day', flight_date,
            coalesce(
                try_to_date('{{ var("as_of_date") }}'), current_date()
            )
        ) as days_since_last_flight,

        coalesce(
            datediff('day', flight_date,
                coalesce(try_to_date('{{ var("as_of_date") }}'), current_date())
            ) > {{ var ('stale_days_threshold') }}, false
        ) as stale_member,

        flight_date is not null as has_flown,

        -- data-quality flags
        (enrollment_date_raw is not null and enrollment_date is null)   as is_enrollment_date_invalid,
        (flight_date_raw is not null and flight_date is null)           as is_flight_date_invalid,
        (country_code <> 'USA' and date_of_birth is null)               as is_dob_missing_or_invalid,
        coalesce(flight_date < enrollment_date, false)                   as is_flight_before_enrollment,

        source_file_name,
        batch_id,
        load_timestamp
    from base
),

final as (

    select
        *,
        array_to_string(array_construct_compact(
            iff(is_enrollment_date_invalid,     'INVALID_ENROLLMENT_DATE',  null),
            iff(is_flight_date_invalid,         'INVALID_FLIGHT_DATE',      null),
            iff(is_dob_missing_or_invalid,      'DOB_MISSING_OR_INVALID',   null),
            iff(is_flight_before_enrollment,    'FLIGHT_BEFORE_ENROLLMENT', null)
        ), ', ') as dq_issues
    from derived
)

select
    *,
    iff(dq_issues = '', 'OK', 'REVIEW') as dq_status
from final