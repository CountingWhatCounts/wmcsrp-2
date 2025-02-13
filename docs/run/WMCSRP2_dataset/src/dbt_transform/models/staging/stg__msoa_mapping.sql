
  
  create view "WMCSRP2"."main_staging"."stg__msoa_mapping__dbt_tmp" as (
    select
    msoa11cd,
    msoa11nm,
    chgind,
    msoa21cd,
    msoa21nm,
    lad22cd,
    lad22nm,
    lad22nmw
from
    "WMCSRP2"."main_preprocessed_data"."seed_msoa_mapping"
  );
