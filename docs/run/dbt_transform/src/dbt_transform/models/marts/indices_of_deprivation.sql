
  
    
    

    create  table
      "dev"."main_marts"."indices_of_deprivation__dbt_tmp"
  
    as (
      select *
from "dev"."main_staging"."stg__indices_of_deprivation"
where
    msoa21cd in (
        select msoa21cd from "dev"."main_warehouse"."int__area_codes"
    )
    );
  
  