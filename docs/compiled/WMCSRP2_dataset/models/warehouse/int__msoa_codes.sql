with

all_msoas as (
    select
        distinct lad22cd,
        msoa21cd,
        msoa21nm
    from
        "wmcsrp2"."public_staging"."stg__msoa_mapping"
),

target_msoas as (
    select * from all_msoas
    where lad22cd in (select lad22cd from "wmcsrp2"."public_warehouse"."int__local_authority_codes")
)

select * from target_msoas