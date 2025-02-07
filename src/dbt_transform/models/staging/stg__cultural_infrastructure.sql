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
    {{ ref('cultural_infrastructure') }}