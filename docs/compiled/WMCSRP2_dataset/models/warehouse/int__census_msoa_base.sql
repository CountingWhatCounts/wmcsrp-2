with

msoa_codes as (
    select
        distinct msoa21cd,
        msoa21nm
    from
        "wmcsrp2"."public_warehouse"."int__msoa_codes"
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
        "wmcsrp2"."public_staging"."stg__census" as t
    inner join
        msoa_codes as wac
    on
        wac.msoa21cd = t.msoa21cd
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