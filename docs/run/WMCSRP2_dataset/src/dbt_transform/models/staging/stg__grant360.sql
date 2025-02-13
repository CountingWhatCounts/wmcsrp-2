
  
  create view "WMCSRP2"."main_staging"."stg__grant360__dbt_tmp" as (
    


select
    amount_applied_for,
    amount_awarded,
    amount_disbursed,
    award_date,
    recipient_org_name,
    recipient_org_postal_code,
    funding_org_name,
    grant_type,
    best_available_district_additional_data,
    best_available_district_geographic_code_additional_data,
    best_available_ward_additional_data,
    best_available_ward_geographic_code_additional_data
from
    "WMCSRP2"."main_preprocessed_data"."seed_grant360"
  );
