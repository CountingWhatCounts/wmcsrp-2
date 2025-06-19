select
    msoa_code,
    income_type,
    amount_£,
    upper_confidence_limit_£,
    lower_confidence_limit_£,
    confidence_interval_£
from {{ ref('stg__annual_household_income') }}