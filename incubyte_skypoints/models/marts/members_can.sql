select
    *
from {{ ref('members') }}
where country_code = 'CAN'