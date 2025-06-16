select
    CAST(REPLACE(pcds, ' ', '') as text) as postcode,
    CAST(oslaua as text) as oslaua,
    CAST(ctry as text) as ctry,
    CAST(rgn as text) as rgn,
    CAST(msoa21 as text) as msoa21cd
from
    "wmcsrp2"."public"."raw__postcode_mapping"