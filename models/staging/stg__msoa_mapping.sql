select
    CAST(msoa11cd as text) as msoa11cd,
    CAST(msoa11nm as text) as msoa11nm,
    CAST(chgind as text) as chgind,
    CAST(msoa21cd as text) as msoa21cd,
    CAST(msoa21nm as text) as msoa21nm,
    CAST(lad22cd as text) as lad22cd,
    CAST(lad22nm as text) as lad22nm,
    CAST(lad22nmw as text) as lad22nmw,
    CAST(objectid as integer) as objectid
from
    {{ source('preprocessed_data', 'raw__msoa_mapping') }}