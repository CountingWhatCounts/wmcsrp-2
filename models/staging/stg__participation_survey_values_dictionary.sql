select
    CAST("variable" as text),
    CAST("key" as text),
    CAST("value" as text)
from
    {{ source('preprocessed_data', 'raw__participation_survey_values_dictionary') }}