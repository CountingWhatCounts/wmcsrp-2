SELECT distinct
    lad23cd,
    local_authority_name,
    age_16_plus_population_count as estimated_age_16_plus_population_count
FROM
    "wmcsrp2"."public_staging"."stg__modelled_participation_statistics"
where protected_characteristic_sub_domain = '16 Plus'