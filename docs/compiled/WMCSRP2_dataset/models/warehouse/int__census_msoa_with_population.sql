with msoa_census as (
    select * from "wmcsrp2"."public_warehouse"."int__census_msoa_base"
),


msoa_population as (
    select * from "wmcsrp2"."public_staging"."stg__msoa_population"
),

combined as (
    select
        msoa_census.msoa21cd,
        msoa21nm,
        content,
        measure,
        count,
        n,
        case
            when n >= population then n+5
            else population
        end as population
    from msoa_census
    join msoa_population on msoa_census.msoa21cd = msoa_population.msoa21cd
)

select * from combined