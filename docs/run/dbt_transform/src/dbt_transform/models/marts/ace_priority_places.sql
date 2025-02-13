
  
    
    

    create  table
      "dev"."main_marts"."ace_priority_places__dbt_tmp"
  
    as (
      select
    *
from
    "dev"."main_staging"."stg__ace_priority_places"
    );
  
  