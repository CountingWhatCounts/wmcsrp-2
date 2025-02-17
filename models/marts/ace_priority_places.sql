select
    lad22cd,
    priority_place
from
    {{ ref('int__ace_priority_places')}}