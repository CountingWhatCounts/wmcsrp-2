select
    CAST("Lookup Code" as text) as lookup_code,
    CAST("LAD23CD" as text) as lad23cd,
    CAST("Local Authority Name" as text) as local_authority_name,
    CAST("Category" as text) as category,
    CAST("Sub-Category" as text) as subcategory,
    CAST("Particiaption Domain" as text) as participation_domain,
    CAST("Protected Characteristic" as text) as protected_characteristic,
    CAST("Protected Characteristic Sub Domain" as text) as protected_characteristic_sub_domain,
    CAST(replace("Not Participated", '%', '') as float) / 100.0 as not_participated,
    CAST(replace("Participated", '%', '') as float) / 100.0 as participated,
    CAST("16 Plus Population Count" as integer) as age_16_plus_population_count,
    CAST("Characteristic Sub Domain Population Count" as integer) as characteristic_sub_domain_population_count,
    CAST(" Estimated Number of People Participating " as integer) as estimated_number_of_people_participating,
    CAST(replace("Percentage of Overall 16 Plus Population", '%', '') as float) / 100.0 as percentage_of_overall_16_plus_population
from
    "wmcsrp2"."public"."raw__modelled_participation_statistics"