select
    CAST(area_codes as text) as area_code,
    CAST(value as float) as value,
    CAST(margin_of_error as text) as margin_of_error,
    CAST(date as text) as date,
    CAST(measure as text) as wellbeing_factor
from
    {{ source('preprocessed_data', 'raw__wellbeing') }}