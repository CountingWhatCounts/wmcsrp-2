select *
from {{ ref('stg__indices_of_deprivation') }}
where
    msoa21cd in (
        select msoa21cd from {{ ref('int__area_codes') }}
    )