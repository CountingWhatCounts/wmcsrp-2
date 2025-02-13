
  
  create view "dev"."main_staging"."stg__ace_priority_places__dbt_tmp" as (
    select
    ladnm,
    priority_place,
    levelling_up_place
from
    "dev"."main"."seed_ace_priority_places"
  );
