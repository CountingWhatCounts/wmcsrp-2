
  
    
    

    create  table
      "dev"."main_marts"."ace_project_grants__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__ace_project_grants_aggregated"
    );
  
  