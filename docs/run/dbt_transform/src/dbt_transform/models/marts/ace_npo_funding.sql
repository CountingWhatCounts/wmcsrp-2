
  
    
    

    create  table
      "dev"."main_marts"."ace_npo_funding__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__ace_npo_funding_aggregated"
    );
  
  