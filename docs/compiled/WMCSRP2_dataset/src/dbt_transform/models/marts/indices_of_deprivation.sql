select *
from "WMCSRP2"."main_staging"."stg__indices_of_deprivation"
where
    msoa21cd in (
        select msoa21cd from "WMCSRP2"."main_warehouse"."int__area_codes"
    )