select
    lad22cd,
    CAST(sum_award_amount as integer)
from {{ ref('int__ace_project_grants_aggregated') }}