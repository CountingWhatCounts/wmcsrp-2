select
    CAST("QUESTION" as text) as question,
    CAST("ANSWER" as text) as answer,
    CAST("LOCAL_AUTHORITY" as text) as local_authority,
    CAST("P" as float) as p,
    CAST("SAMPLE_N" as integer) as n
from
    {{ source('preprocessed_data', 'raw__residents_survey_local_authority_results') }}