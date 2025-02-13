
  
    
    

    create  table
      "WMCSRP2"."main_marts"."ace_project_grants__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__ace_project_grants_aggregated"
    );
  
  