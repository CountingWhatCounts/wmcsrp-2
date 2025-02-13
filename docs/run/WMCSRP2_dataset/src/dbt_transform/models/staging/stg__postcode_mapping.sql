
  
  create view "WMCSRP2"."main_staging"."stg__postcode_mapping__dbt_tmp" as (
    select
    pcd,
    REPLACE(pcd2, ' ', '') as pcd_no_space,
    pcds,
    oa01,
    lsoa01,
    msoa01,
    oa11,
    lsoa11,
    msoa11,
    lat,
    long,
    oa21,
    lsoa21,
    msoa21
from
    "WMCSRP2"."main_preprocessed_data"."seed_postcode_mapping"
  );
