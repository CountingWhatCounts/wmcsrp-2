select
    lad22cd,
    measure,
    value,
    margin_of_error
from {{ ref('int__economic') }}