with

wellbeing_data as (
    select
        area_codes,
        measure,
        value,
        margin_of_error 
    from
        "WMCSRP2"."main_staging"."stg__wellbeing"
),

wmca_area_codes as (
    select
        lad22cd,
        lad22nm
    from
        "WMCSRP2"."main_warehouse"."int__area_codes"
)

select
    distinct ac.lad22cd,
    ac.lad22nm,
    wd.measure,
    wd.value,
    wd.margin_of_error,
from
    wmca_area_codes as ac
left join
    wellbeing_data as wd
on
    wd.area_codes = ac.lad22cd