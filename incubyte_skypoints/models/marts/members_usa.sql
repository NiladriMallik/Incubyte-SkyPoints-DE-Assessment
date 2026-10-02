{{
    config(
        materialized='view'
    )
}}

select
    *
from {{ ref('members') }}
where country_code = 'USA'