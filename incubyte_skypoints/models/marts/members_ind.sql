select
    *
from {{ ref('members') }}
where country_code = 'IND'