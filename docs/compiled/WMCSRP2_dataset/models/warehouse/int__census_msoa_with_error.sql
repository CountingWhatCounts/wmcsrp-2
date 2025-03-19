with msoa_census as (
    select
        msoa21cd,
        msoa21nm,
        content,
        measure,
        cast(count as float),
        cast(n as float),
        population
    from "wmcsrp2"."public_warehouse"."int__census_msoa_with_population"
),

msoa_with_p as (
    select
        msoa21cd,
        msoa21nm,
        content as census_question,
        measure as answer,
        count as count_of_answer,
        n as sample_size,
        population as population_size,
        count / n as p
    from
        msoa_census
),

msoa_errors as (
    select
        msoa21cd,
        msoa21nm,
        census_question,
        answer,
        count_of_answer,
        sample_size,
        population_size,
        CAST(p as float),
        CAST(1.96 * sqrt( (p * (1-p)) / ((population_size - 1) * sample_size / (population_size - sample_size)) ) as float) as margin_of_error
    from
        msoa_with_p
)


select * from msoa_errors