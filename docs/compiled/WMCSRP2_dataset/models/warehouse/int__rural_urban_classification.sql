with

ruc as (
    select * from "WMCSRP2"."md_raw"."raw__rural_urban_classification"
),

msoa_mapping as (
    select * from "WMCSRP2"."md_raw"."raw__msoa_mapping"
),

combined as (
    select
        ruc.msoa11cd,
        ruc.msoa11nm,
        ruc11cd,
        ruc11
    from ruc
    left join msoa_mapping
    on ruc.msoa11cd = msoa_mapping.msoa11cd
)

select * from combined