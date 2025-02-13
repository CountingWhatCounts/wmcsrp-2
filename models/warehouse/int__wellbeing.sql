with

wellbeing_data as (
    select
        area_codes,
        wellbeing_factor,
        value,
        margin_of_error 
    from
        {{ ref('raw__wellbeing') }}
),

wmca_area_codes as (
    select
        lad22cd,
        lad22nm
    from
        {{ ref('int__area_codes') }}
)

select
    distinct ac.lad22cd,
    ac.lad22nm,
    wd.wellbeing_factor,
    wd.value,
    wd.margin_of_error,
from
    wmca_area_codes as ac
left join
    wellbeing_data as wd
on
    wd.area_codes = ac.lad22cd