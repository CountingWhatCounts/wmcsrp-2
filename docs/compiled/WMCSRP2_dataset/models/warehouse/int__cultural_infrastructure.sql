with

lad_codes as (
    select distinct lad22nm, lad22cd
    from "WMCSRP2"."md_warehouse"."int__area_codes"
),

services_msoa as (
    select
        area_name as ladnm,
        msoa21,
        postcode,
        service_type,
        name as service_name,
        source,
        category,
    from "WMCSRP2"."md_raw"."raw__cultural_infrastructure" as ci
    left join "WMCSRP2"."md_raw"."raw__postcode_mapping" as pm
    on ci.postcode = pm.pcd_no_space
),

services_lad as (
    select
        *
    from services_msoa
    join lad_codes on services_msoa.ladnm = lad_codes.lad22nm
)

select * from services_lad