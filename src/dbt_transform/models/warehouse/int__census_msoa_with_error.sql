{{ config(
    tags=["wmca_project_warehouse", "census", "wmca_project"]
) }}


with msoa_census as (
    select * from {{ ref('int__census_msoa_with_population') }}
),


msoa_errors as (
    select
        msoa21cd,
        msoa21nm,
        content,
        measure,
        count as count_of_answer,
        n as sample_size,
        population as population_size,
        count / n as p,
        1.96 * sqrt( (p * (1-p)) / ((population - 1) * n / (population - n)) ) as margin_of_error
    from
        msoa_census
)


select * from msoa_errors