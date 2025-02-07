select
    ladnm,
    priority_place,
    levelling_up_place
from
    {{ ref('ace_priority_places') }}