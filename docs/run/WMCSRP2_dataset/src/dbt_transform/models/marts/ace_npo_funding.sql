
  
    
    

    create  table
      "WMCSRP2"."main_marts"."ace_npo_funding__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__ace_npo_funding_aggregated"
    );
  
  