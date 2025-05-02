select
    CAST("RGN24CD" as text) as rgn24cd,
    CAST("RGN24NM" as text) as rgn24nm
from
    {{ source('preprocessed_data', 'raw__region_mapping') }}