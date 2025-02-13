
  
  create view "WMCSRP2"."main_staging"."stg__ace_priority_places__dbt_tmp" as (
    select
    ladnm,
    priority_place,
    levelling_up_place
from
    "WMCSRP2"."main_preprocessed_data"."seed_ace_priority_places"
  );
