
  
    
    

    create  table
      "dev"."main_marts"."grant360__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__grant360"
    );
  
  