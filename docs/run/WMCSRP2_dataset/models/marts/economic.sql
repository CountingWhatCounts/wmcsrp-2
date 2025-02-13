
  
    
    

    create  table
      "WMCSRP2"."md_marts"."economic__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__economic"
    );
  
  