
  
  create view "dev"."main_staging"."stg__economic__dbt_tmp" as (
    select
    id,
    local_authority,
    measure,
    value,
    margin_of_error
from
    "dev"."main"."seed_economic"
  );
