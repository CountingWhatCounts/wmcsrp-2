select distinct
    msoa21cd,
    census_question,
    answer,
    CAST(count_of_answer as integer),
    CAST(sample_size as integer),
    population_size,
    CAST(p as float),
    CAST(margin_of_error as float)
from {{ ref('int__census_msoa_with_error') }}