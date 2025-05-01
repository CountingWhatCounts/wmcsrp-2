select
    CAST("Question" as text) as question,
    CAST("Answer" as text) as answer,
    CAST(local_authority as text) as local_authority,
    CAST(percentage as float) as percentage,
    CAST(n as integer) as n
from
    {{ source('preprocessed_data', 'raw__residents_survey_local_authority_results') }}