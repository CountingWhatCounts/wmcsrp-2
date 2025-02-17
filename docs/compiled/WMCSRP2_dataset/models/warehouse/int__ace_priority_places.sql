with

priority_places as (
    select * from "wmcsrp2"."public_staging"."stg__ace_priority_places"
),

local_authority_codes as (
    select * from "wmcsrp2"."public_warehouse"."int__local_authority_codes"
),

combined as (
    select
        lad22cd,
        priority_place
    from priority_places join local_authority_codes
    on priority_places.lad22nm = local_authority_codes.lad22nm
)

select * from combined