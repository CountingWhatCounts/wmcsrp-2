select
    msoa11cd,
    msoa11nm,
    chgind,
    msoa21cd,
    msoa21nm,
    lad22cd,
    lad22nm,
    lad22nmw,
    objectid
from
    {{ ref('msoa_mapping') }}