with

project_grants as (
    select * from {{ ref('stg__ace_project_grants_funding') }}
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
        {{ ref('int__local_authority_codes') }}
),


combined as (
    select
        distinct lad22cd,
        lad22nm,
        case
            when sum_award_amount is null then 0
            else sum_award_amount
        end as sum_award_amount
    from local_authority_codes
    left join aggregated on local_authority_codes.lad22nm = aggregated.local_authority
)


select * from combined