select
    lad22cd,
    wellbeing_factor,
    value,
    margin_of_error
from {{ ref('int__wellbeing') }}