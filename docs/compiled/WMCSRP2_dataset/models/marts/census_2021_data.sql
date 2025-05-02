select distinct
    area_type,
    area_code,
    census_question,
    answer,
    CAST(count_of_answer as integer),
    CAST(sample_size as integer),
    population_size,
    CAST(p as float),
    CAST(margin_of_error as float)
from "wmcsrp2"."public_warehouse"."int__census_with_error"