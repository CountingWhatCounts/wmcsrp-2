select
    area_type,
    area_code,
    area_name,
    parent_area_code,
    parent_area_name
from {{ ref('int__area_codes_melted') }}