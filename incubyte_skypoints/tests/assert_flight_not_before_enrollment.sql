select
    country_code,
    member_id,
    enrollment_date,
    flight_date
from {{ ref('stg_member_feed') }}
where flight_date < enrollment_date