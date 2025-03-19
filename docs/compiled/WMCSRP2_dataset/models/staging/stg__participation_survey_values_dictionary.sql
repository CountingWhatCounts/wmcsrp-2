select
    CAST("variable" as text),
    CAST("key" as text),
    CAST("value" as text)
from
    "wmcsrp2"."public"."raw__participation_survey_values_dictionary"