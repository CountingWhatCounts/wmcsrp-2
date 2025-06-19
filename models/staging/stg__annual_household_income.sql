select
    cast("MSOA code" as text) as msoa_code,
    cast("Type" as text) as income_type,
    cast("Amount (£)" as integer) as amount_£,
    cast("Upper confidence limit (£)" as integer) as upper_confidence_limit_£,
    cast("Lower confidence limit (£)" as integer) as lower_confidence_limit_£,
    cast("Confidence interval (£)" as integer) as confidence_interval_£
from
    {{ source('preprocessed_data', 'raw__annual_household_income') }}