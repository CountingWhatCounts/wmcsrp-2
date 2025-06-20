select distinct
    area_type,
    area_code,
    census_question,
    answer,
    CAST(count_of_answer as integer) as count_of_answer,
    CAST(sample_size as integer) as sample_size,
    population_size,
    CAST(p as float) as p,
    CAST(margin_of_error as float) as margin_of_error
from "wmcsrp2"."public_warehouse"."int__census_with_error"