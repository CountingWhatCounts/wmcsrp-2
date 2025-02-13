
  
    
    

    create  table
      "dev"."main_marts"."economic__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__economic"
    );
  
  