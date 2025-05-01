with

local_authority_codes as (
    select distinct lad22nm, lad22cd
    from {{ ref('int__local_authority_codes') }}
),

services_msoa as (
    select
        area_name as ladnm,
        msoa21cd,
        ci.postcode,
        service_type,
        name as service_name,
        source,
        category
    from {{ ref('stg__cultural_infrastructure') }} as ci
    left join {{ ref('stg__postcode_mapping') }} as pm
    on ci.postcode = pm.postcode
),

services_lad as (
    select
        lad22cd,
        lad22nm,
        msoa21cd,
        postcode,
        service_type,
        category,
        source,
        service_name
    from services_msoa
    join local_authority_codes on services_msoa.ladnm = local_authority_codes.lad22nm
)

select * from services_lad