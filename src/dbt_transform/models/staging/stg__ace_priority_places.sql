select
    ladnm,
    priority_place,
    levelling_up_place
from
    {{ ref('seed_ace_priority_places') }}