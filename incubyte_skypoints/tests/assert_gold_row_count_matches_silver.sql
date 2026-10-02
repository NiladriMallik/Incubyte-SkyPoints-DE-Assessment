with silver as(
    select count(*) as n from {{ ref('int_member_enriched') }}
),

gold as(
    select
        (select count(*) from {{ ref('members_aus') }}) +
        (select count(*) from {{ ref('members_ind') }}) +
        (select count(*) from {{ ref('members_can') }}) +
        (select count(*) from {{ ref('members_phl') }}) +
        (select count(*) from {{ ref('members_usa') }}) as n
)
select
    silver.n as silver_rows,
    gold.n as gold_rows
from silver, gold
where silver.n <> gold.n