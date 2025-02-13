
  
    
    

    create  table
      "WMCSRP2"."main_marts"."grant360__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__grant360"
    );
  
  