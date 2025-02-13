
  
    
    

    create  table
      "WMCSRP2"."md_marts"."census__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__census_msoa_with_error"
    );
  
  