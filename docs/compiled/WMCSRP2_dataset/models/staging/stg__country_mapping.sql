select
    CAST("CTRY24CD" as text) as ctry24cd,
    CAST("CTRY24NM" as text) as ctry24nm
from
    "wmcsrp2"."public"."raw__country_mapping"