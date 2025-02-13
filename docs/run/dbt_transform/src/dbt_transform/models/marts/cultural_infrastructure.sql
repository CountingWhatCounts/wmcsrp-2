
  
    
    

    create  table
      "dev"."main_marts"."cultural_infrastructure__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__cultural_infrastructure_msoa"
    );
  
  