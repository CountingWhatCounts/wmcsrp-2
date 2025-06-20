with

wellbeing_data as (
    select
        area_code,
        wellbeing_factor,
        cast(value as float) as value,
        margin_of_error 
    from
        "wmcsrp2"."public_staging"."stg__wellbeing"
    where
        date = 'April 2022 to March 2023'
),

area_codes as (
    select
        lad22cd as area_code,
        lad22nm as area_name
    from
        "wmcsrp2"."public_warehouse"."int__local_authority_codes"
    union all
        select
            'E12000005' as area_code,
            'West Midlands' as area_name
    union all
        select
            'E92000001' as area_code,
            'England' as area_name
)

select
    distinct ac.area_code,
    ac.area_name,
    wd.wellbeing_factor,
    CAST(wd.value as float) as value,
    wd.margin_of_error
from
    area_codes as ac
left join
    wellbeing_data as wd
on
    wd.area_code = ac.area_code