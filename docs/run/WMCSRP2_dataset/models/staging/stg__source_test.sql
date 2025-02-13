
  
  create view "WMCSRP2"."main_staging"."stg__source_test__dbt_tmp" as (
    select * from "WMCSRP2"."gcs_data"."ace_project_grants"
  );
