select
    msoa21cd,
    msoa21nm,
    local_authority,
    CAST(imd_score as float),
    imd_decile_msoa,
    imd_quantile_msoa,
    msoa_2021_status
from {{ ref('stg__indices_of_deprivation') }}
where
    msoa21cd in (
        select msoa21cd from {{ ref('int__msoa_codes') }}
    )