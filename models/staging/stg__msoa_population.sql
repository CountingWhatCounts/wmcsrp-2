select
    CAST(msoa_2021_code as text) as msoa21cd,
    CAST(total as integer) as population
from
    {{ source('preprocessed_data', 'raw__msoa_population') }}