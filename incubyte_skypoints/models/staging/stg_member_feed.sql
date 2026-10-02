with source as (
    select * from {{ source('raw', 'member_feed') }}
    where raw_record:"Record_Type"::string = 'D' -- drops header and any trailing rows
),

unified as(
    select
        source_file_name,
        batch_id,
        row_number,
        load_timestamp,

        trim(raw_record:"Member_Id"::string)            as member_id,
        trim(raw_record:"Member_Name"::string)          as member_name,
        upper(trim(raw_record:"Tier_Code"::string))     as tier_code,
        trim(raw_record:"Agent_Name"::string)           as agent_name,
        upper(trim(raw_record:"State"::string))         as state,
        upper(trim(raw_record:"Country"::string))       as country_raw,
        raw_record:"Enrollment_Date"::string            as enrollment_date_raw,
        raw_record:"Last_Flight_Date"::string           as flight_date_raw,
        raw_record:"DOB"::string                        as dob_raw,
        upper(trim(raw_record:"Is_Active"::string))     as is_active_raw
    from source       

),

parsed as(
    select
        u.*,
        coalesce(m.country_code, 'UNKNOWN')             as country_code,
        try_to_date(u.enrollment_date_raw, 'YYYYMMDD')  as enrollment_date,
        try_to_date(u.flight_date_raw, 'YYYYMMDD')      as flight_date,
        try_to_date(u.dob_raw, 'DDMMYYYY')              as date_of_birth,
        (u.is_active_raw = 'A')                         as is_active
    from unified u left join {{ ref('country_map') }} m
    on u.country_raw = m.source_code
)

select
    *
from parsed
qualify row_number() over (
    partition by member_id
    order by load_timestamp desc, source_file_name desc, row_number desc
) = 1

