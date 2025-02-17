select
    CAST(msoa_2021_code as text) as msoa21cd,
    CAST(total as integer) as population
from
    "wmcsrp2"."public"."raw__msoa_population"