
  
    
    

    create  table
      "WMCSRP2"."md_marts"."cultural_infrastructure__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__cultural_infrastructure_msoa"
    );
  
  