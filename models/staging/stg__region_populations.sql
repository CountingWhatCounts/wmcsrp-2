select
    CAST("Code" as text) as code,
    CAST("Name" as text) as name,
    CAST("Geography" as text) as geography,
    CAST(REPLACE(TRIM("All ages"), ',', '') as integer) as population
from
    {{ source('preprocessed_data', 'raw__region_populations') }}