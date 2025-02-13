
  
  create view "WMCSRP2"."main_staging"."stg__rural_urban_classification__dbt_tmp" as (
    SELECT
    MSOA11CD as msoa11cd,
    MSOA11NM as msoa11nm,
    RUC11CD as ruc11cd,
    RUC11 as ruc11
FROM
    "WMCSRP2"."main_preprocessed_data"."seed_rural_urban_classification"
  );
