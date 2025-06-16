select
    CAST("RGN24CD" as text) as rgn24cd,
    CAST("RGN24NM" as text) as rgn24nm
from
    "wmcsrp2"."public"."raw__region_mapping"