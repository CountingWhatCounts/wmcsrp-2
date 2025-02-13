
  
  create view "WMCSRP2"."main_staging"."stg__cultural_infrastructure__dbt_tmp" as (
    select
    area_name as ladnm,
    replace(postcode, ' ', '') as postcode,
    service_type,
    name,
    source,
    category,
    lead_partner_organisation_,
    funding_year,
    website
from
    "WMCSRP2"."main_preprocessed_data"."seed_cultural_infrastructure"
  );
