with

residents_survey as (
    select * from "wmcsrp2"."public_staging"."stg__residents_survey_local_authority_results"
),

lad_codes as (
    select * from "wmcsrp2"."public_warehouse"."int__local_authority_codes"
),

population_size as (
    select distinct code, population from "wmcsrp2"."public_staging"."stg__region_populations"
),

combined as (
    select * from residents_survey
    join lad_codes on residents_survey.local_authority = lad_codes.lad22nm
    join population_size on lad_codes.lad22cd = population_size.code
)

select
    lad22cd,
    question,
    answer,
    percentage as p,
    n,
    population,
    CAST(1.96 * sqrt( (percentage * (1-percentage)) / ((population - 1) * n / (population - n)) ) as float) as margin_of_error
from combined