
  
    
    

    create  table
      "WMCSRP2"."main_marts"."area_codes__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__area_codes"
    );
  
  