select
    CAST(local_authority as text) as lad22nm
from
    {{ source('preprocessed_data', 'raw__ace_levelling_up_for_culture_places') }}