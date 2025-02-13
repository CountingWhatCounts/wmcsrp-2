
  
  create view "dev"."main_staging"."stg__cultural_infrastructure__dbt_tmp" as (
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
    "dev"."main"."seed_cultural_infrastructure"
  );
