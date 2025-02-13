
  
    
    

    create  table
      "dev"."main_marts"."rural_urban_classification__dbt_tmp"
  
    as (
      select * from "dev"."main_warehouse"."int__rural_urban_classification"
    );
  
  