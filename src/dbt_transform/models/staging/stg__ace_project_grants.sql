select
    ace_area,
    activity_name,
    award_amount,
    award_date,
    decision_month,
    decision_quarter,
    local_authority,
    main_discipline,
    recipient,
    strand
from
    {{ ref('ace_project_grants') }}