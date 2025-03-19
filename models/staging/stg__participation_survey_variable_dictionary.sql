select
    CAST(lower("variable_name") as text),
    CAST("variable_label" as text)
from
    {{ source('preprocessed_data', 'raw__participation_survey_variable_dictionary') }}