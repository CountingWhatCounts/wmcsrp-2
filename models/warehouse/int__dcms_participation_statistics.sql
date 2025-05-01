with

participation_rename as (
    select
        case
            when participation_type = 'Did not participate in the arts in person in the last 12 months'
                then 'Participated in Creative Activities'
            when participation_type = 'Did not attend arts events in person in the last 12 months'
                then 'Attended Cultural Events in Person'
            else participation_type
        end as participation_type,
        response_group,
        response_breakdown,
        (percentage_of_respondents_2023_24 / 100) as percentage_of_respondents_2023_24,
        (percentage_of_respondents_2023_24_lower_estimate / 100) as percentage_of_respondents_2023_24_lower_estimate,
        (percentage_of_respondents_2023_24_upper_estimate / 100) as percentage_of_respondents_2023_24_upper_estimate,
        number_of_respondents_2023_24,
        number_of_respondents_2023_24_base
    from
        {{ ref('stg__dcms_participation_statistics') }}
),

value_fix AS (
    SELECT
        participation_type,
        response_group,
        response_breakdown,
        CASE
            WHEN participation_type IN ('Participated in Creative Activities', 'Attended Cultural Events in Person')
                THEN 1 - percentage_of_respondents_2023_24
            ELSE percentage_of_respondents_2023_24
        END AS percentage_of_respondents_2023_24,
        CASE
            WHEN participation_type IN ('Participated in Creative Activities', 'Attended Cultural Events in Person')
                THEN 1 - percentage_of_respondents_2023_24_upper_estimate
            ELSE percentage_of_respondents_2023_24_lower_estimate
        END AS percentage_of_respondents_2023_24_lower_estimate,
        CASE
            WHEN participation_type IN ('Participated in Creative Activities', 'Attended Cultural Events in Person')
                THEN 1 - percentage_of_respondents_2023_24_lower_estimate
            ELSE percentage_of_respondents_2023_24_upper_estimate
        END AS percentage_of_respondents_2023_24_upper_estimate,
        number_of_respondents_2023_24,
        number_of_respondents_2023_24_base
    FROM participation_rename
)

select * from value_fix