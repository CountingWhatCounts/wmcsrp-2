
  
    
    

    create  table
      "WMCSRP2"."main_marts"."ace_priority_places__dbt_tmp"
  
    as (
      select
    *
from
    "WMCSRP2"."main_staging"."stg__ace_priority_places"
    );
  
  