
  
    
    

    create  table
      "WMCSRP2"."md_marts"."wellbeing__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__wellbeing"
    );
  
  