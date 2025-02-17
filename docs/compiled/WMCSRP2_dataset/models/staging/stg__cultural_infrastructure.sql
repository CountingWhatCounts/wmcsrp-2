select
    CAST(area_name as text) as area_name,
    CAST(postcode as text) as postcode,
    CAST(service_type as text) as service_type,
    CAST(name as text) as name,
    CAST(source as text) as source,
    CAST(website as text) as website,
    CAST(category as text) as category,
    CAST(funding_year as text) as funding_year,
    CAST(lead_partner_organisation_ as text) as lead_partner_organisation
from
    "wmcsrp2"."public"."raw__cultural_infrastructure"