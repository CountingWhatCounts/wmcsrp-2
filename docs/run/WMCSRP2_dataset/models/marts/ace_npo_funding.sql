
  
    
    

    create  table
      "WMCSRP2"."md_marts"."ace_npo_funding__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__ace_npo_funding_aggregated"
    );
  
  