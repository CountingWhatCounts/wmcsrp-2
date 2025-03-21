select distinct
    CAST(applicant_name as text) as applicant_name,
    CAST(type_of_organisation__npo_ipso_transfer_ as text) as type_of_organisation,
    CAST(national_youth_music_organisation as text) as national_youth_music_organisation,
    CAST("2018-22_average_annual_funding__figure_accurate_at_april_2018_" as integer) as average_annual_funding_2018_22,
    CAST("2022_23_annual_funding__extension_year_" as integer) as annual_funding__extension_year_2022_23,
    CAST("2023-26_annual_funding__offered_4_nov_2022_" as integer) as annual_funding__offered_4_nov_2022_2023_26,
    CAST(main_discipline as text) as main_discipline,
    CAST(ace_area as text) as ace_area,
    CAST(ons_region as text) as ons_region,
    CAST(constituency as text) as constituency,
    case 
        when local_authority = 'Herefordshire, County of' then 'Herefordshire'
        else local_authority
    end as local_authority
from
    "wmcsrp2"."public"."raw__ace_npo_funding"