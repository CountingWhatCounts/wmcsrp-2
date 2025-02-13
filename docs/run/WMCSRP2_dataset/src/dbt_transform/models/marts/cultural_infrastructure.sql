
  
    
    

    create  table
      "WMCSRP2"."main_marts"."cultural_infrastructure__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__cultural_infrastructure_msoa"
    );
  
  