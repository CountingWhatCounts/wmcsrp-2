
  
  create view "dev"."main_staging"."stg__msoa_population__dbt_tmp" as (
    select
    msoa_2021_code as msoa21cd,
    total as population
from
    "dev"."main"."seed_msoa_population"
  );
