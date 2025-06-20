select
    msoa21cd,
    msoa21nm,
    local_authority,
    CAST(imd_score as float) as imd_score,
    imd_decile_msoa,
    imd_quantile_msoa,
    msoa_2021_status
from "wmcsrp2"."public_staging"."stg__indices_of_deprivation"
where
    msoa21cd in (
        select msoa21cd from "wmcsrp2"."public_warehouse"."int__msoa_codes"
    )