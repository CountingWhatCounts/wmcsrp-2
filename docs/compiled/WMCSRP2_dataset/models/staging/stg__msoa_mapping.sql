select
    CAST(msoa21cd as text) as msoa21cd,
    CAST(msoa21nm as text) as msoa21nm,
    CAST(lad22cd as text) as lad22cd,
    CAST(lad22nm as text) as lad22nm
from
    "wmcsrp2"."public"."raw__msoa_mapping"