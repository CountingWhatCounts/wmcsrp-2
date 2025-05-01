with


msoa_census as (
    select * from "wmcsrp2"."public_warehouse"."int__census_base"
    where area_type = 'msoa'
),

msoa_population as (
    select
        msoa21cd as area_code,
        population
    from "wmcsrp2"."public_staging"."stg__msoa_population"
),

census_msoa_with_population as (
    select
        msoa_census.area_type,
        msoa_census.area_code,
        area_name,
        content,
        measure,
        count,
        n,
        p,
        population
    from msoa_census
    join msoa_population on msoa_census.area_code = msoa_population.area_code
),

other_census as (
    select * from "wmcsrp2"."public_warehouse"."int__census_base"
    where area_type != 'msoa'
),

other_population as (
    select distinct
        code,
        population
    from "wmcsrp2"."public_staging"."stg__region_populations"
),

other_msoa_with_population as (
    select
        other_census.area_type,
        other_census.area_code,
        area_name,
        content,
        measure,
        count,
        n,
        p,
        population
    from other_census
    join other_population on other_census.area_code = other_population.code
)

select * from census_msoa_with_population
union all
select * from other_msoa_with_population