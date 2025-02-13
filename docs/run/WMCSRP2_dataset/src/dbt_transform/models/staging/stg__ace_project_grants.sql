
  
  create view "WMCSRP2"."main_staging"."stg__ace_project_grants__dbt_tmp" as (
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
    "WMCSRP2"."main_preprocessed_data"."seed_ace_project_grants"
  );
