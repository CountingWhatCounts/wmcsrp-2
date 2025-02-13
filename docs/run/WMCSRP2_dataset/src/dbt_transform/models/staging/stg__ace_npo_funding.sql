
  
  create view "WMCSRP2"."main_staging"."stg__ace_npo_funding__dbt_tmp" as (
    select
    applicant_name,
    type_of_organisation__npo_ipso_transfer_,
    national_youth_music_organisation,
    "2018-22_average_annual_funding__figure_accurate_at_april_2018_" as average_annual_funding_2018_22,
    "2022_23_annual_funding__extension_year_" as annual_funding__extension_year_2022_23,
    "2023-26_annual_funding__offered_4_nov_2022_" as annual_funding__offered_4_nov_2022_2023_26,
    main_discipline,
    ace_area,
    ons_region,
    constituency,
    local_authority,
    levelling_up_for_culture_place,
    priority_place
from
    "WMCSRP2"."main_preprocessed_data"."seed_ace_npo_funding"
  );
