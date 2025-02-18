with

local_authority_codes as (
    select distinct lad22nm, lad22cd
    from "wmcsrp2"."public_warehouse"."int__local_authority_codes"
),

services_msoa as (
    select
        area_name as ladnm,
        msoa21cd,
        pm.postcode,
        service_type,
        name as service_name,
        source,
        category
    from "wmcsrp2"."public_staging"."stg__cultural_infrastructure" as ci
    left join "wmcsrp2"."public_staging"."stg__postcode_mapping" as pm
    on ci.postcode = pm.postcode
),

services_lad as (
    select
        *
    from services_msoa
    join local_authority_codes on services_msoa.ladnm = local_authority_codes.lad22nm
)

select * from services_lad