with

economic_data as (
    select
        local_authority,
        measure,
        value,
        margin_of_error
    from
        "wmcsrp2"."public_staging"."stg__economic"
),

local_authority_codes as (
    select
        lad22cd,
        lad22nm
    from
        "wmcsrp2"."public_warehouse"."int__local_authority_codes"
)

select
    distinct ac.lad22cd,
    ac.lad22nm,
    ed.measure,
    CAST(round(ed.value::numeric, 5) as float) as value,
    case
        when margin_of_error < 0.05 then '<5%'
        when margin_of_error < 0.1 then '5-10%'
        when margin_of_error < 0.2 then '10-20%'
    end as margin_of_error
from
    economic_data as ed
inner join
    local_authority_codes as ac
on
    ed.local_authority = ac.lad22nm