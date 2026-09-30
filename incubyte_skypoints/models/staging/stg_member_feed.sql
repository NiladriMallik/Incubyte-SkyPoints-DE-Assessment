with source as (
    select * from {{ source('raw', 'member_feed') }}
),

unified as(
    select
        country_code,
        source_file_name,
        batch_id,
        row_number,
        load_timestamp,

        -- cast each key to STRING before coalesce
        coalesce(raw_record:"Unique ID"::string, raw_record:"ID"::string)                           as member_id,
        coalesce(raw_record:"Member Name"::string, raw_record:"Name"::string)                       as member_name,
        upper(trim(coalesce(raw_record:"Tier Type"::string, raw_record:"TierCode"::string)))        as tier_code,
        raw_record:"Individual or Corporate"::string                                                as member_type_raw,
        coalesce(raw_record:"Date of Birth"::string, raw_record:"DOB"::string)                      as dob_raw,
        coalesce(raw_record:"Date of Enrollment"::string, raw_record:"EnrollmentDate"::string)      as enrollment_date_raw,
        coalesce(raw_record:"Date of Flight"::string,
                 raw_record:"Flight Date"::string,
                 raw_record:"FlightDate"::string
                 )                                                                                  as flight_date_raw,

    from source       

),

parsed as(
    select
        *,
        case country_code
            when 'AUS' then try_to_date(dob_raw, 'YYYY-MM-DD')
            when 'IND' then try_to_date(dob_raw, 'MM/DD/YYYY')
        end as date_of_birth,

        case country_code
            when 'AUS' then try_to_date(enrollment_date_raw, 'YYYY-MM-DD')
            when 'IND' then try_to_date(enrollment_date_raw, 'MM/DD/YYYY')
            when 'USA' then try_to_date(lpad(enrollment_date_raw, 8, '0'), 'MMDDYYYY')
        end as enrollment_date,

        case country_code
            when 'AUS' then try_to_date(flight_date_raw, 'YYYY-MM-DD')
            when 'IND' then try_to_date(flight_date_raw, 'MM/DD/YYYY')
            when 'USA' then try_to_date(lpad(flight_date_raw, 8, '0'), 'MMDDYYYY')
        end as flight_date,

        case member_type_raw
            when 'I' then 'INDIVIDUAL'
            when 'C' then 'CORPORATE'
        end as member_type
    from unified
)

select
    country_code,
    member_id,
    member_name,
    tier_code,
    member_type,
    date_of_birth,
    enrollment_date,
    enrollment_date_raw,
    flight_date,
    flight_date_raw,
    source_file_name,
    batch_id,
    row_number,
    load_timestamp
from parsed
