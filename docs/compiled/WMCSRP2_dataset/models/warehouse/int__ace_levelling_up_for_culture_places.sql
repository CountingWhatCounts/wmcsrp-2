with

levelling_up as (
    select
        lad22nm,
        TRUE as levelling_up_place
    from "wmcsrp2"."public_staging"."stg__ace_levelling_up_for_culture_places"
),

lad_codes as (
    select
        lad22cd,
        lad22nm
    from
        "wmcsrp2"."public_warehouse"."int__local_authority_codes"
),

combined as (
    select
        lad22cd,
        levelling_up_place
    from levelling_up
    left join lad_codes
    on lad_codes.lad22nm = levelling_up.lad22nm
    where lad22cd is not null
)

select * from combined