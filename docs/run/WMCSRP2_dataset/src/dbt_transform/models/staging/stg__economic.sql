
  
  create view "WMCSRP2"."main_staging"."stg__economic__dbt_tmp" as (
    select
    id,
    local_authority,
    measure,
    value,
    margin_of_error
from
    "WMCSRP2"."main_preprocessed_data"."seed_economic"
  );
