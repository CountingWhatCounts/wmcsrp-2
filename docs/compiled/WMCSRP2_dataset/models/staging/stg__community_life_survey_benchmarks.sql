select
    CAST(metric as text) as metric,
    CAST(benchmark as text) as benchmark,
    CAST("2023/24 Percentage (%)" as text) as percentage_of_respondents,
    CAST("2023/24 Lower Estimate percentage (%)" as float) as percentage_lower,
    CAST("2023/24 Upper Estimate percentage (%)" as float) as percentage_upper,
    CAST("2023/24 Number of respondents" as integer) as number_of_respondents,
    CAST("2023/24 Unweighted Base - number of people aged 16 and over" as integer) as unweighted_base
from
    "wmcsrp2"."public"."raw__community_life_survey_benchmarks"