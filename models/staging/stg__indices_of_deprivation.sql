select
    CAST(msoa21cd as text) as msoa21cd,
    CAST(msoa21nm as text) as msoa21nm,
    CAST(local_authority as text) as local_authority,
    CAST(imd_score as float) as imd_score,
    CAST(imd_decile_msoa as integer) as imd_decile_msoa,
    CAST(imd_quantile_msoa as integer) as imd_quantile_msoa,
    CAST(msoa_2021_status as text) as msoa_2021_status,
from
    {{ ref('seed_indices_of_deprivation') }}