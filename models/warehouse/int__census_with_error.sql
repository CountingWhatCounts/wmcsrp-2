with census as (
    select
        area_type,
        area_code,
        area_name,
        content as census_question,
        measure as answer,
        cast(count as float) as count_of_answer,
        cast(n as float) as sample_size,
        population as population_size,
        p
    from {{ ref('int__census_with_population') }}
),

census_errors as (
    select
        area_type,
        area_code,
        area_name,
        census_question,
        answer,
        count_of_answer,
        sample_size,
        population_size,
        p,
        case
            when sample_size >= population_size then CAST(1.96 * sqrt( ((p * (1-p)) / sample_size) * (1 / (population_size - 1)) ) as float)
        else CAST(1.96 * sqrt( ((p * (1-p)) / sample_size) * ((population_size - sample_size) / (population_size - 1)) ) as float)
        end as margin_of_error
    from
        census
)


select distinct * from census_errors