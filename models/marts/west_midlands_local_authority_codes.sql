select
    lad22cd,
    lad22nm,
    area
from {{ ref('int__local_authority_codes') }}