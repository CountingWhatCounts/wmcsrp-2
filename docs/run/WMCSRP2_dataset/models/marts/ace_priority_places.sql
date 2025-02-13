
  
    
    

    create  table
      "WMCSRP2"."md_marts"."ace_priority_places__dbt_tmp"
  
    as (
      select
    *
from
    "WMCSRP2"."md_raw"."raw__ace_priority_places"
    );
  
  