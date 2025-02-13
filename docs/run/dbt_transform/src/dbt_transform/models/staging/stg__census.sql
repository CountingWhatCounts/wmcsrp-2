
  
  create view "dev"."main_staging"."stg__census__dbt_tmp" as (
    SELECT
    content,
    date as year,
    geography,
    geography_code,
    n,
    measure,
    count
FROM
    "dev"."main"."seed_census"
  );
