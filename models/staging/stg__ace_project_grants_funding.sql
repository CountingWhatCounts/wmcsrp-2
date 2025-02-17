select
    CAST(recipient as text) as recipient,
    CAST(activity_name as text) as activity_name,
    CAST(award_amount as integer) as award_amount,
    CAST(award_date as date) as award_date,
    CAST(decision_month as text) as decision_month,
    CAST(decision_quarter as text) as decision_quarter,
    CAST(ace_area as text) as ace_area,
    CAST(local_authority as text) as local_authority,
    CAST(main_discipline as text) as main_discipline,
    CAST(strand as text) as strand,
    CAST("time-limited_priority" as text) as time_limited_priority
from
    {{ source('preprocessed_data', 'raw__ace_project_grants') }}