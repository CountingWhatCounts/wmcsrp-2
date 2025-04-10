with

wellbeing_data as (
    select
        area_codes,
        wellbeing_factor,
        value,
        margin_of_error 
    from
        {{ ref('stg__wellbeing') }}
    where
        date = 'April 2022 to March 2023'
),

local_authority_codes as (
    select
        lad22cd,
        lad22nm
    from
        {{ ref('int__local_authority_codes') }}
)

select
    distinct ac.lad22cd,
    ac.lad22nm,
    wd.wellbeing_factor,
    CAST(wd.value as float),
    wd.margin_of_error
from
    local_authority_codes as ac
left join
    wellbeing_data as wd
on
    wd.area_codes = ac.lad22cd