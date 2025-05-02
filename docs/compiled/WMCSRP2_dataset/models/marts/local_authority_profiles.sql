with

-- Local Authorities
wm_lads as (
	select lad22cd, lad22nm
    from "wmcsrp2"."public_warehouse"."int__local_authority_codes"
),

-- Population
population_table as (
    select * from "wmcsrp2"."public_staging"."stg__region_populations"
),


-- Households
households as (
    select
        lad22cd,
        sum(sample_size) as number_of_households
    from "wmcsrp2"."public_marts"."census_2021_data" as a
    join "wmcsrp2"."public_marts"."west_midlands_msoa_codes" as b
    on a.area_code = b.area_code
    where census_question = 'Number of households'
    group by lad22cd
),

-- NPO funding
npo_funding as (
    select lad22cd, sum_annual_funding_2023_2026
    from "wmcsrp2"."public_marts"."ace_npo_funding"
),

-- Project Grants Funding
project_grants_funding as (
    select lad22cd, sum_award_amount
    from "wmcsrp2"."public_marts"."ace_project_grants_funding"
),

-- Community Life Metrics
community_life_metrics as (
    select
        lad23cd,

        MAX(percentage_of_respondents) FILTER (
            WHERE metric = 'How much do you agree or disagree with the following statements? - I am proud to live in my local area : Agree'
        ) as proud_to_live_locally,

        MAX(percentage_of_respondents) FILTER (
            WHERE metric = 'Overall, how satisfied or dissatisfied are you with your local area as a place to live? : Satisfied'
        ) as local_area_satisfaction,

        MAX(percentage_of_respondents) FILTER (
            WHERE metric = 'How strongly do you feel you belong to your immediate neighbourhood?: Total'
        ) as neighbourhood_belonging,

        MAX(percentage_of_respondents) FILTER (
            WHERE metric = 'To what extent do you agree or disagree that this local area is a place where people from different backgrounds get on well together?: Agree'
        ) as social_cohesion,

        MAX(percentage_of_respondents) FILTER (
            WHERE metric = 'How much do you agree or disagree with the following statements? - I would recommend my local area to others as a good place to live: Agree'
        ) as area_recommendation

    from "wmcsrp2"."public_marts"."community_life_survey"

    where metric IN (
        'How much do you agree or disagree with the following statements? - I am proud to live in my local area : Agree',
        'Overall, how satisfied or dissatisfied are you with your local area as a place to live? : Satisfied',
        'How strongly do you feel you belong to your immediate neighbourhood?: Total',
        'To what extent do you agree or disagree that this local area is a place where people from different backgrounds get on well together?: Agree',
        'How much do you agree or disagree with the following statements? - I would recommend my local area to others as a good place to live: Agree'
    )

    group by lad23cd
),

-- Priority Places
priority_places as (
    select distinct lad22cd, priority_place
    from "wmcsrp2"."public_marts"."ace_priority_places"
),

-- Levelling up for culture places
levelling_up as (
    select distinct lad22cd, levelling_up_place
    from "wmcsrp2"."public_marts"."ace_levelling_up_for_culture_places"
),

-- Wellbeing
wellbeing as (
    select
        area_code,

        MAX(value) FILTER (
            WHERE wellbeing_factor = 'Happiness means'
        ) as happiness_mean,

        MAX(value) FILTER (
            WHERE wellbeing_factor = 'Worthwhile means'
        ) as worthwhile_mean,

        MAX(value) FILTER (
            WHERE wellbeing_factor = 'Anxiety means'
        ) as anxiety_mean,

        MAX(value) FILTER (
            WHERE wellbeing_factor = 'Life satisfaction means'
        ) as life_satisfaction_mean

    from "wmcsrp2"."public_marts"."annual_population_survey_wellbeing_estimates"

    group by area_code
),

-- Participation
participation_table as (
    select
        lad23cd,

        MAX(value) FILTER (
            WHERE participation_type = 'Attended Cultural Events in Person'
        ) as events,

        MAX(margin_of_error) FILTER (
            WHERE participation_type = 'Attended Cultural Events in Person'
        ) as events_error,

        MAX(value) FILTER (
            WHERE participation_type = 'Participated in Creative Activities'
        ) as participation,

        MAX(margin_of_error) FILTER (
            WHERE participation_type = 'Participated in Creative Activities'
        ) as participation_error,

        MAX(value) FILTER (
            WHERE participation_type = 'Engaged Online with Culture and Arts'
        ) as online,

        MAX(margin_of_error) FILTER (
            WHERE participation_type = 'Engaged Online with Culture and Arts'
        ) as online_error

    from "wmcsrp2"."public_marts"."modelled_participation_statistics"

    where demographic = 'Total Engaged'
    and participation_type in (
        'Attended Cultural Events in Person',
        'Participated in Creative Activities',
        'Engaged Online with Culture and Arts'
    )

    group by lad23cd
),

-- Residents Survey - where creative
where_attended_culture as (
    SELECT
        lad22cd,

        SUM(p) FILTER (
            WHERE question = 'Where you attended arts and culture events in the last 12 months'
        ) AS attended_events_local,

        SUM(p) FILTER (
            WHERE question = 'Where you took part in creative activities in the last 12 months'
        ) AS participated_creative_activities_local,

        SUM(p) FILTER (
            WHERE question = 'Where you visited arts and culture places in the last 12 months'
        ) AS visited_culture_places_local

    FROM "wmcsrp2"."public_marts"."residents_survey_local_authority_results"

    WHERE question IN (
        'Where you attended arts and culture events in the last 12 months',
        'Where you took part in creative activities in the last 12 months',
        'Where you visited arts and culture places in the last 12 months'
    )
    AND answer IN (
        'All in $profileoslaua',
        'Mostly in $profileoslaua, with some events elsewhere'
    )

    GROUP BY lad22cd
),

-- Residents Survey - desire for creativity
desire_for_creativity as (
    SELECT
        lad22cd,

        SUM(p) FILTER (
            WHERE question = 'Being creative in day-to-day life'
        ) AS desire_to_be_more_creative

    FROM "wmcsrp2"."public_marts"."residents_survey_local_authority_results"

    WHERE question IN (
        'Being creative in day-to-day life'
    )

    AND answer IN (
        'I am not able to be as creative as I want to be and would like to do more',
        'I am able to be creative in my day-to-day life, but would like to do more'
    )

    GROUP BY lad22cd
),


-- Residents Survey - feeling creative
feeling_creative as (
    SELECT
        lad22cd,

        SUM(p) FILTER (
            WHERE question = 'Whether you feel you are a creative person'
        ) AS feeling_you_are_creative

    FROM "wmcsrp2"."public_marts"."residents_survey_local_authority_results"

    WHERE question IN (
        'Whether you feel you are a creative person'
    )
    
    AND answer IN (
        'A lot',
        'A little'
    )

    GROUP BY lad22cd
),

combined as (
    select
        wm_lads.lad22cd,
        wm_lads.lad22nm,
        population_table.population,
        households.number_of_households,
        npo_funding.sum_annual_funding_2023_2026,
        project_grants_funding.sum_award_amount,
        community_life_metrics.proud_to_live_locally,
        community_life_metrics.local_area_satisfaction,
        community_life_metrics.neighbourhood_belonging,
        community_life_metrics.social_cohesion,
        community_life_metrics.area_recommendation,
        priority_places.priority_place,
        levelling_up.levelling_up_place,
        wellbeing.happiness_mean,
        wellbeing.worthwhile_mean,
        wellbeing.anxiety_mean,
        wellbeing.life_satisfaction_mean,
        participation_table.participation,
        participation_table.participation_error,
        participation_table.events,
        participation_table.events_error,
        participation_table.online,
        participation_table.online_error,
        where_attended_culture.attended_events_local,
        where_attended_culture.participated_creative_activities_local,
        where_attended_culture.visited_culture_places_local,
        desire_for_creativity.desire_to_be_more_creative,
        feeling_creative.feeling_you_are_creative
    from wm_lads
    join population_table on wm_lads.lad22cd = population_table.code
    join households on wm_lads.lad22cd = households.lad22cd
    join npo_funding on wm_lads.lad22cd = npo_funding.lad22cd
    join project_grants_funding on wm_lads.lad22cd = project_grants_funding.lad22cd
    join community_life_metrics on wm_lads.lad22cd = community_life_metrics.lad23cd
    join priority_places on wm_lads.lad22cd = priority_places.lad22cd
    join levelling_up on wm_lads.lad22cd = levelling_up.lad22cd
    join wellbeing on wm_lads.lad22cd = wellbeing.area_code
    join participation_table on wm_lads.lad22cd = participation_table.lad23cd
    join where_attended_culture on wm_lads.lad22cd = where_attended_culture.lad22cd
    join desire_for_creativity on wm_lads.lad22cd = desire_for_creativity.lad22cd
    join feeling_creative on wm_lads.lad22cd = feeling_creative.lad22cd
)

select * from combined