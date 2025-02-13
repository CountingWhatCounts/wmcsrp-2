
  
    
    

    create  table
      "WMCSRP2"."md_marts"."grant360__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__grant360"
    );
  
  