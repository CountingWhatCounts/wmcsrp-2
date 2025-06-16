select distinct
    metric,
    local_authority,
    percentage_of_respondents,
    percentage_lower,
    percentage_upper,
    number_of_respondents,
    unweighted_base,
    lad23cd,
    itl2nm
from "wmcsrp2"."public_staging"."stg__community_life_survey" a
join "wmcsrp2"."public_warehouse"."int__area_codes" b
on a.lad23cd = b.lad22cd