
  
    
    

    create  table
      "WMCSRP2"."main_marts"."wellbeing__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__wellbeing"
    );
  
  