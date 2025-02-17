select
    CAST(ladnm as text) as lad22nm,
    CAST(priority_place as boolean) as priority_place
from
    {{ source('preprocessed_data', 'raw__ace_priority_places') }}