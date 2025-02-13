
  
    
    

    create  table
      "WMCSRP2"."md_marts"."area_codes__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__area_codes"
    );
  
  