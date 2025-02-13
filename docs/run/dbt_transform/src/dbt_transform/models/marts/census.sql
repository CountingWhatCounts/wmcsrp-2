
  
    
    

    create  table
      "dev"."main_marts"."census__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__census_msoa_with_error"
    );
  
  