select
    lad22cd,
    'msoa21cd' as code_type,
    msoa21cd as area_code,
    msoa21nm as area_name
from "wmcsrp2"."public_warehouse"."int__msoa_codes"
union all
select
    'n/a' as lad22cd,
    'region' as code_type,
    'E12000005' as area_code,
    'West Midlands' as area_name
union all
select
    'n/a' as lad22cd,
    'country' as code_type,
    'E92000001' as area_code,
    'England' as area_name