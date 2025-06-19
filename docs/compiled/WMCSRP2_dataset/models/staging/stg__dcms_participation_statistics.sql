select
    CAST("Participation Type" as text) as participation_type,
    CAST("Response Group" as text) as response_group,
    CAST("Response Breakdown" as text) as response_breakdown,
    CAST("Percentage of respondents 2023/24" as float) as percentage_of_respondents_2023_24,
    CAST("Percentage of respondents 2023/24 Lower estimate" as float) as percentage_of_respondents_2023_24_lower_estimate,
    CAST("Percentage of respondents 2023/24 Upper estimate" as float) as percentage_of_respondents_2023_24_upper_estimate,
    CAST("2023/24 No. of respondents" as int) as number_of_respondents_2023_24,
    CAST("2023/24 Base" as int) as number_of_respondents_2023_24_base
from
    "wmcsrp2"."public"."raw__dcms_participation_statistics"