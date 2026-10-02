{{
    config(
        materialized='incremental',
        unique_key='member_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('raw', 'member_feed') }}
    where raw_record:"Record_Type"::string = 'D' -- drops header and any trailing rows
    {% if is_incremental() %}
      and load_timestamp > (
        select coalesce(max(load_timestamp), '1900-01-01'::timestamp_ntz) from {{ this }}
      )
    {% endif %}
),

unified as(
    select
        source_file_name,
        batch_id,
        row_number,
        load_timestamp,

        to_date(regexp_substr(source_file_name, '[0-9]{8}'), 'YYYYMMDD') as file_date,
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
        (u.is_active_raw = 'A')                         as is_active,
        md5(concat_ws('|',
            u.member_name,
            u.tier_code,
            u.agent_name,
            u.state,
            u.country_raw,
            u.enrollment_date_raw,
            u.flight_date_raw,
            u.dob_raw,
            u.is_active_raw
        )) as row_hash
    from unified u left join {{ ref('country_map') }} m
    on u.country_raw = m.source_code
),

latest_in_batch as (
    select * from parsed
    qualify row_number() over (
        partition by member_id
        order by
            file_date desc nulls last,
            load_timestamp desc,
            source_file_name desc,
            row_number desc
    ) = 1
)

select
    l.*,
    current_timestamp()::timestamp_ntz as _updated_at,
    {% if is_incremental() %}
        case
            when t.member_id is not null and t.country_code <> l.country_code then t.country_code
            else t.previous_country_code
        end as previous_country_code
    from latest_in_batch l
    left join {{ this }} t
        on t.member_id = l.member_id
    where t.member_id is null
        or (t.row_hash <> l.row_hash
        and coalesce(l.file_date, '1900-01-01') >= coalesce(t.file_date, '1900-01-01'))
    {% else %}
    null::varchar as previous_country_code
    from latest_in_batch l
    {% endif %}

