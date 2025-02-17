select
    lad22cd,
    levelling_up_place
from
    {{ ref('int__ace_levelling_up_for_culture_places')}}