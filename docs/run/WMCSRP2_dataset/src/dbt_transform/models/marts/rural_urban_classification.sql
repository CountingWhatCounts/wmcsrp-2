
  
    
    

    create  table
      "WMCSRP2"."main_marts"."rural_urban_classification__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."main_warehouse"."int__rural_urban_classification"
    );
  
  