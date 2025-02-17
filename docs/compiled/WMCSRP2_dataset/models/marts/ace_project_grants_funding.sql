select
    lad22cd,
    CAST(sum_award_amount as integer)
from "wmcsrp2"."public_warehouse"."int__ace_project_grants_aggregated"