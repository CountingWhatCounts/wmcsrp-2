select
    msoa21cd,
    ruc11cd,
    ruc11
from {{ ref('int__rural_urban_classification') }}