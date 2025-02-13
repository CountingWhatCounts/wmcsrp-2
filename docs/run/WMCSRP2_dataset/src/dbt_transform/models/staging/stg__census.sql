
  
  create view "WMCSRP2"."main_staging"."stg__census__dbt_tmp" as (
    SELECT
    content,
    date as year,
    geography,
    geography_code,
    n,
    measure,
    count
FROM
    "WMCSRP2"."main_preprocessed_data"."seed_census"
  );
