
  
  create view "WMCSRP2"."main_staging"."stg__msoa_population__dbt_tmp" as (
    select
    msoa_2021_code as msoa21cd,
    total as population
from
    "WMCSRP2"."main_preprocessed_data"."seed_msoa_population"
  );
