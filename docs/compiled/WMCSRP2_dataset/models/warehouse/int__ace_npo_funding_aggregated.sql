with npo_funding as (
    select
        local_authority,
        annual_funding__extension_year_2022_23,
        annual_funding__offered_4_nov_2022_2023_26,
        average_annual_funding_2018_22
    from "wmcsrp2"."public_staging"."stg__ace_npo_funding"
),


aggregated as (
    select
        sum(annual_funding__extension_year_2022_23) as sum_annual_funding_extension_year,
        sum(annual_funding__offered_4_nov_2022_2023_26) as sum_annual_funding_2023_2026,
        sum(average_annual_funding_2018_22) as sum_average_annual_funding_2018_2022,
        local_authority
    from
        npo_funding
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
        coalesce(sum_annual_funding_extension_year, 0) as sum_annual_funding_extension_year,
        coalesce(sum_annual_funding_2023_2026, 0) as sum_annual_funding_2023_2026,
        coalesce(sum_average_annual_funding_2018_2022, 0) as sum_average_annual_funding_2018_2022
    from local_authority_codes
    left join aggregated on local_authority_codes.lad22nm = aggregated.local_authority
),


final as (
    select
        lad22cd,
        lad22nm,
        CAST(sum_annual_funding_extension_year as INTEGER) as sum_annual_funding_extension_year,
        CAST(sum_annual_funding_2023_2026 as INTEGER) as sum_annual_funding_2023_2026,
        CAST(sum_average_annual_funding_2018_2022 as INTEGER) as sum_average_annual_funding_2018_2022
    from combined
)


select * from final