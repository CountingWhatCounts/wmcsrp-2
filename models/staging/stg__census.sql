select
    CAST(geography_code as text) as msoa21cd,
    CAST(n as integer) as n,
    CAST(measure as text) as measure,
    CAST(count as integer) as count,
    CAST(content as text) as content
from
    {{ source('preprocessed_data', 'raw__census') }}