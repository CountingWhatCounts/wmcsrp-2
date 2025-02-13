

select
    msoa21cd,
    msoa21nm,
    local_authority,
    imd_score,
    imd_decile_msoa,
    imd_quantile_msoa,
    msoa_2021_status
from
    "WMCSRP2"."main_preprocessed_data"."seed_indices_of_deprivation"