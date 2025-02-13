
  
    
    

    create  table
      "WMCSRP2"."main_marts"."economic__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__economic"
    );
  
  