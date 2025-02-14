select
    CAST(LADNM as text) as lad22nm,
    CAST(LEVELLING_UP_PLACE as boolean) as levelling_up_place
from
    {{ ref('seed_ace_levelling_up_for_culture_places') }}