
  
    
    

    create  table
      "WMCSRP2"."main_marts"."census__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__census_msoa_with_error"
    );
  
  