select
    CAST(metric as text) as metric,
    CAST(local_authority as text) as local_authority,
    CAST("2023_24_percentage_" as float) as percentage_of_respondents,
    CAST("2023_24_lower_estimate_percentage_" as float) as percentage_lower,
    CAST("2023_24_upper_estimate_percentage_" as float) as percentage_upper,
    CAST("2023_24_number_of_respondents" as integer) as nubmer_of_respondents,
    CAST("2023_24_unweighted_base_-_number_of_people_aged_16_and_over" as integer) as unweighted_base,
    CAST(lad23_code as text) as lad23cd,
    CAST(itl2_name as text) as itl2nm
from
    {{ source('preprocessed_data', 'raw__community_life_survey') }}