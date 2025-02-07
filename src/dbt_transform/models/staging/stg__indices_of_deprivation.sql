{{ config(
    tags=["uk_data_staging"]
) }}

select
    msoa21cd,
    msoa21nm,
    local_authority,
    imd_score,
    imd_decile_msoa,
    imd_quantile_msoa,
    msoa_2021_status
from
    {{ ref('indices_of_deprivation') }}