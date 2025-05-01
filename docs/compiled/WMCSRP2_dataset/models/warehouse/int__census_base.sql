with

msoa_codes as (
    select distinct
        msoa21cd as area_code,
        msoa21nm as area_name
    from
        "wmcsrp2"."public_warehouse"."int__msoa_codes"
),

msoa_census as (
    select
        t.area_type,
        wac.area_code,
        wac.area_name,
        t.content,
        t.measure,
        t.count as count,
        t.n as n
    from
        "wmcsrp2"."public_staging"."stg__census" as t
    inner join
        msoa_codes as wac
    on
        wac.area_code = t.area_code
),

local_authority_codes as (
    select distinct
        lad22cd as area_code,
        lad22nm as area_name
    from
        "wmcsrp2"."public_warehouse"."int__local_authority_codes"
),

local_authority_census as (
    select
        'local authority' as area_type,
        wac.area_code,
        wac.area_name,
        t.content,
        t.measure,
        t.count as count,
        t.n as n
    from
        "wmcsrp2"."public_staging"."stg__census" as t
    inner join
        local_authority_codes as wac
    on
        wac.area_code = t.area_code
),

region_census as (
    select
        'region' as area_type,
        'E12000005' as area_code,
        'West Midlands' as area_name,
        t.content,
        t.measure,
        t.count as count,
        t.n as n
    from "wmcsrp2"."public_staging"."stg__census" as t
    where t.area_code = 'E12000005'
),

combined as (
    select * from msoa_census
    union all
    select * from local_authority_census
    union all
    select * from region_census
)

select
    area_type,
    area_code,
    area_name,
    content,
    rtrim(trim(regexp_replace(measure, '[\s;]*measures: Value$', '')), ';') as measure,
    count,
    n,
    cast(count as float) / cast(n as float) as p
from
    combined