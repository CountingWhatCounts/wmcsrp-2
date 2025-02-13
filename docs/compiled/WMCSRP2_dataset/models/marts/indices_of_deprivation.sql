select *
from "WMCSRP2"."md_raw"."raw__indices_of_deprivation"
where
    msoa21cd in (
        select msoa21cd from "WMCSRP2"."md_warehouse"."int__area_codes"
    )