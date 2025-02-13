
  
    
    

    create  table
      "WMCSRP2"."md_marts"."ace_project_grants__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__ace_project_grants_aggregated"
    );
  
  