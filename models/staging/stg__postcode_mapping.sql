select
    CAST(REPLACE(pcds, ' ', '') as text) as postcode,
    CAST(msoa11 as text) as msoa11cd,
    CAST(msoa21 as text) as msoa21cd
from
    {{ source('preprocessed_data', 'raw__postcode_mapping') }}