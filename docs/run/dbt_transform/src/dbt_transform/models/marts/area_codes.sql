
  
    
    

    create  table
      "dev"."main_marts"."area_codes__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__area_codes"
    );
  
  