
  
    
    

    create  table
      "WMCSRP2"."md_marts"."rural_urban_classification__dbt_tmp"
  
    as (
      select * from "WMCSRP2"."md_warehouse"."int__rural_urban_classification"
    );
  
  