select
    lad22cd,
    msoa21cd,
    msoa21nm
from {{ ref('int__msoa_codes') }}