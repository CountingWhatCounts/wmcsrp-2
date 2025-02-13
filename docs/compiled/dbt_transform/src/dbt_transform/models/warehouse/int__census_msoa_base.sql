with

wmca_area_codes as (
    select
        distinct msoa21cd,
        msoa21nm
    from
        "dev"."main_warehouse"."int__area_codes"
),

msoa_census as (
    select
        wac.msoa21cd,
        wac.msoa21nm,
        t.content,
        t.measure,
        t.count as count,
        t.n as n
    from
        "dev"."main_staging"."stg__census" as t
    inner join
        wmca_area_codes as wac
    on
        wac.msoa21cd = t.geography_code
)

select
    msoa21cd,
    msoa21nm,
    content,
    rtrim(trim(regexp_replace(measure, '[\s;]*measures: Value$', '')), ';') as measure,
    count,
    n
from
    msoa_census