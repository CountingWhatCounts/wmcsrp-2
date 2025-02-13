


with npo_funding as (
    select * from "dev"."main_staging"."stg__ace_npo_funding"
),


aggregated as (
    select
        sum(annual_funding__extension_year_2022_23) as sum_annual_funding_extension_year,
        sum(annual_funding__offered_4_nov_2022_2023_26) as sum_annual_funding_2023_2026,
        sum(average_annual_funding_2018_22) sum_average_annual_funding_2018_2022,
        local_authority
    from
        npo_funding
    group by
        local_authority
),

msoa_mapping as (
    select
        lad22cd,
        lad22nm
    from
        "dev"."main_warehouse"."int__area_codes"
),


combined as (
    select
        distinct lad22cd,
        lad22nm,
        case
            when sum_annual_funding_extension_year is null then 0
            else sum_annual_funding_extension_year
        end as sum_annual_funding_extension_year,
        case
            when sum_annual_funding_2023_2026 is null then 0
            else sum_annual_funding_2023_2026
        end as sum_annual_funding_2023_2026,
        case
            when sum_average_annual_funding_2018_2022 is null then 0
            else sum_average_annual_funding_2018_2022
        end as sum_average_annual_funding_2018_2022,
    from msoa_mapping
    left join aggregated on msoa_mapping.lad22nm = aggregated.local_authority
)


select * from combined