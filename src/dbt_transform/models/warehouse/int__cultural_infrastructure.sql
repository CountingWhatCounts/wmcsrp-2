with

lad_codes as (
    select distinct lad22nm, lad22cd
    from {{ ref('int__area_codes') }}
),

services_msoa as (
    select
        ladnm,
        msoa21,
        postcode,
        service_type,
        name as service_name,
        source,
        category,
    from {{ ref('stg__cultural_infrastructure') }} as ci
    left join {{ ref('stg__postcode_mapping') }} as pm
    on ci.postcode = pm.pcd_no_space
),

services_lad as (
    select
        *
    from services_msoa
    join lad_codes on services_msoa.ladnm = lad_codes.lad22nm
)

select * from services_lad