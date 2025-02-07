with

ruc as (
    select * from {{ ref('stg__rural_urban_classification') }}
),

msoa_mapping as (
    select * from {{ ref('stg__msoa_mapping') }}
),

combined as (
    select
        msoa21cd,
        msoa21nm
        ruc11cd,
        ruc11
    from ruc
    left join msoa_mapping
    on ruc.msoa11cd = msoa_mapping.msoa11cd
)

select * from combined