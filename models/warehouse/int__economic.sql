with

economic_data as (
    select
        local_authority,
        measure,
        value,
        margin_of_error
    from
        {{ ref('raw__economic') }}
),

wmca_area_codes as (
    select
        lad22cd,
        lad22nm,
    from
        {{ ref('int__area_codes') }}
)

select
    distinct ac.lad22cd,
    ac.lad22nm,
    ed.measure,
    round(ed.value, 5) as value,
    case
        when margin_of_error < 0.05 then '<5%'
        when margin_of_error < 0.1 then '5-10%'
        when margin_of_error < 0.2 then '10-20%'
    end as margin_of_error
from
    economic_data as ed
inner join
    wmca_area_codes as ac
on
    ed.local_authority = ac.lad22nm