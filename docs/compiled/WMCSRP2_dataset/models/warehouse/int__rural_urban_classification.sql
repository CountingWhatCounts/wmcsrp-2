with

ruc as (
    select * from "wmcsrp2"."public_staging"."stg__rural_urban_classification"
),

msoa_mapping as (
    select * from "wmcsrp2"."public_staging"."stg__msoa_mapping"
),

combined as (
    select
        msoa21cd,
        msoa21nm,
        ruc11cd,
        ruc11
    from ruc
    left join msoa_mapping
    on ruc.msoa11cd = msoa_mapping.msoa21cd
),

filtered as (
    select * from combined
    where msoa21cd in (select msoa21cd from "wmcsrp2"."public_warehouse"."int__msoa_codes")
)

select * from filtered