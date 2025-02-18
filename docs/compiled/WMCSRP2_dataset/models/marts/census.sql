select
    msoa21cd,
    census_question,
    answer,
    count_of_answer,
    sample_size,
    population_size,
    CAST(p as float),
    CAST(margin_of_error as float)
from "wmcsrp2"."public_warehouse"."int__census_msoa_with_error"