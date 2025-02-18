select
    CAST("MSOA11CD" as text) as msoa11cd,
    CAST("MSOA11NM" as text) as msoa11nm,
    CAST("RUC11CD" as text) as ruc11cd,
    CAST("RUC11" as text) as ruc11
from
    "wmcsrp2"."public"."raw__rural_urban_classification"