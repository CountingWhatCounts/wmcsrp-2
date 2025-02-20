select
    msoa21cd,
    amount_awarded,
    award_date,
    recipient_org_name,
    recipient_org_postal_code,
    funding_org_name
from {{ ref('int__360giving') }}