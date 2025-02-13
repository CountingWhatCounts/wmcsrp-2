
  
    
    

    create  table
      "dev"."main_marts"."wellbeing__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__wellbeing"
    );
  
  