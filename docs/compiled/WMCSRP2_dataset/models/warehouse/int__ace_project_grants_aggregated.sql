with

project_grants as (
    select * from "wmcsrp2"."public_staging"."stg__ace_project_grants_funding"
),


aggregated as (
    select
        sum(award_amount) as sum_award_amount,
        local_authority
    from
        project_grants
    group by
        local_authority
),


local_authority_codes as (
    select
        lad22cd,
        lad22nm
    from
        "wmcsrp2"."public_warehouse"."int__local_authority_codes"
),


combined as (
    select
        distinct lad22cd,
        lad22nm,
        coalesce(sum_award_amount, 0) as sum_award_amount
    from local_authority_codes
    left join aggregated on local_authority_codes.lad22nm = aggregated.local_authority
)


select * from combined