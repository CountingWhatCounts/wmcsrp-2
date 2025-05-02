select
    CAST("CTRY24CD" as text) as ctry24cd,
    CAST("CTRY24NM" as text) as ctry24nm
from
    {{ source('preprocessed_data', 'raw__country_mapping') }}