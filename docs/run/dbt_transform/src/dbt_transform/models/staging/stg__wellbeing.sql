
  
  create view "dev"."main_staging"."stg__wellbeing__dbt_tmp" as (
    SELECT
    id,
    date,
    area_codes,
    measure,
    value,
    margin_of_error
FROM
    "dev"."main"."seed_wellbeing"
where
    date in ('April 2022 to March 2023')
  );
