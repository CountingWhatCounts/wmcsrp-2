select
    CAST("Participation Type" as text) as participation_type,
    CAST("Response Group" as text) as response_group,
    CAST("Response Breakdown " as text) as response_breakdown,
    CAST("Change between 2022/23 and 2023/24" as text) as change_between_2022_2023_and_2024,
    CAST("Percentage of respondents 2023/24" as float) as percentage_of_respondents_2023_24,
    CAST("Percentage of respondents 2023/24 Lower estimate" as float) as percentage_of_respondents_2023_24_lower_estimate,
    CAST("Percentage of respondents 2023/24 Upper estimate" as float) as percentage_of_respondents_2023_24_upper_estimate,
    CAST("2023/24 No. of respondents" as int) as number_of_respondents_2023_24,
    CAST("2023/24 Base" as int) as number_of_respondents_2023_24_base,
    CAST("Percentage of respondents 2022/23" as float) as percentage_of_respondents_2022_23,
    CAST("Percentage of respondents 2022/23 Lower estimate" as float) as percentage_of_respondents_2022_23_lower_estimate,
    CAST("Percentage of respondents 2022/23 Upper estimate" as float) as percentage_of_respondents_2022_23_upper_estimate,
    CAST("2022/23 No. of respondents" as int) as number_of_respondents_2022_23,
    CAST("2022/23 Base" as int) as number_of_respondents_2022_23_base
from
    "wmcsrp2"."public"."raw__dcms_participation_statistics"