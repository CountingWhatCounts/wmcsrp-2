with

-- Local Authorities
wm_lads as (
	select lad22cd, lad22nm, area
    from {{ ref("west_midlands_local_authority_codes") }}
),

-- Population


population_table as (
    select * from {{ ref('stg__region_populations') }}
),

estimated_populations as (
    select
        lad23cd,
        estimated_age_16_plus_population_count
    from {{ ref('int__estimated_16_plus_population') }}
),

-- Households
households as (
    select
        lad22cd,
        sum(sample_size) as number_of_households
    from {{ ref("census_2021_data") }} as a
    join {{ ref("west_midlands_msoa_codes") }} as b
    on a.area_code = b.area_code
    where census_question = 'Number of households'
    group by lad22cd
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

    from {{ ref("community_life_survey") }}

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
    from {{ ref("ace_priority_places") }}
),

-- Levelling up for culture places
levelling_up as (
    select distinct lad22cd, levelling_up_place
    from {{ ref("ace_levelling_up_for_culture_places") }}
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

    from {{ ref("annual_population_survey_wellbeing_estimates") }}

    group by area_code
),

-- Participation


participation_table as (
    select
        lad23cd,
        

        MAX(participated) FILTER (
            WHERE participation_domain = 'Any Participation in Creative Activities'
        ) as participating_in_creative_activities,

        MAX(participated) FILTER (
            WHERE participation_domain = 'All In Person Events'
        ) as attending_in_person_events,

        MAX(participated) FILTER (
            WHERE participation_domain = 'All Creative Activities and Media Consumption'
        ) as all_creative_activities_and_media_consumption,

        MAX(participated) FILTER (
            WHERE participation_domain = 'All Cultural Events'
        ) as attending_or_watching_cultural_events,

        MAX(participated) FILTER (
            WHERE participation_domain = 'Any Library Engagement'
        ) as any_library_engagement,

        MAX(participated) FILTER (
            WHERE participation_domain = 'Any Media Consumption'
        ) as media_consumption,

        MAX(participated) FILTER (
            WHERE participation_domain = 'Any Museums and Galleries Engagement'
        ) as museums_and_galleries,

        MAX(participated) FILTER (
            WHERE participation_domain = 'Any Cultural Places Engagement'
        ) as any_cultural_places_engagement,

        MAX(participated) FILTER (
            WHERE participation_domain = 'All Streamed/Online/Digital Events'
        ) as streamed_online_digital_events,

        MAX(participated) FILTER (
            WHERE participation_domain = 'Any Heritage Engagement'
        ) as heritage

    from {{ ref("stg__modelled_participation_statistics") }}

    where protected_characteristic_sub_domain = '16 Plus'
    and participation_domain in (
        'Any Media Consumption',
        'Any Library Engagement',
        'All Streamed/Online/Digital Events',
        'Any Museums and Galleries Engagement',
        'Any Participation in Creative Activities',
        'Any Heritage Engagement',
        'All In Person Events',
        'All Cultural Events',
        'All Creative Activities and Media Consumption',
        'Any Cultural Places Engagement'
    )

    group by lad23cd
),

-- Residents Survey - where creative


where_attended_culture as (
    SELECT
        lad22cd,

        SUM(p) FILTER (
            WHERE question = 'Where did you attend the cultural events?'
        ) AS attended_events_local,

        SUM(p) FILTER (
            WHERE question = 'Where did you do the creative activities?'
        ) AS participated_creative_activities_local,

        SUM(p) FILTER (
            WHERE question = 'Where did you visit the cultural places?'
        ) AS visited_culture_places_local

    FROM {{ ref("int__residents_survey") }}

    WHERE question IN (
        'Where did you attend the cultural events?',
        'Where did you do the creative activities?',
        'Where did you visit the cultural places?'
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
            WHERE question = 'Which ONE of the following best applies to you in terms of being creative in your day to day life'
        ) AS desire_to_be_more_creative

    FROM {{ ref("int__residents_survey") }}

    WHERE question IN (
        'Which ONE of the following best applies to you in terms of being creative in your day to day life'
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
            WHERE question = 'To what extent do you feel that you are a creative person?'
        ) AS feeling_you_are_creative

    FROM {{ ref("int__residents_survey") }}

    WHERE question IN (
        'To what extent do you feel that you are a creative person?'
    )
    
    AND answer IN (
        'A lot',
        'A little'
    )

    GROUP BY lad22cd
),

expectation_to_attend as (
    SELECT
        lad22cd,

        SUM(p) FILTER (
            WHERE question = 'Compared with the last 12 months, in the coming 12 months do you expect to attend these kinds of events'
        ) AS expectation_to_attend_events_next_12_months,

        SUM(p) FILTER (
            WHERE question = 'Compared with the last 12 months, in the coming 12 months do you expect to go to these kinds of places'
        ) AS expectation_to_visit_places_next_12_months,

        SUM(p) FILTER (
            WHERE question = 'Compared with the last 12 months, in the coming 12 months do you expect to take part in these kinds of activities'
        ) AS expectation_to_participate_in_activities_next_12_months

    FROM {{ ref("int__residents_survey") }}

    WHERE question IN (
        'Compared with the last 12 months, in the coming 12 months do you expect to attend these kinds of events',
        'Compared with the last 12 months, in the coming 12 months do you expect to go to these kinds of places',
        'Compared with the last 12 months, in the coming 12 months do you expect to take part in these kinds of activities'
    )
    
    AND answer IN (
        'About the same amount',
        'A lot more in the next 12 months',
        'A little more in the next 12 months'
    )

    GROUP BY lad22cd
),

combined as (
    select
        wm_lads.lad22cd,
        wm_lads.lad22nm,
        wm_lads.area as in_wmca,
        population_table.population,
        estimated_populations.estimated_age_16_plus_population_count,
        households.number_of_households,
        community_life_metrics.proud_to_live_locally as proud_to_live_locally,
        community_life_metrics.local_area_satisfaction,
        community_life_metrics.neighbourhood_belonging,
        community_life_metrics.social_cohesion,
        community_life_metrics.area_recommendation,
        priority_places.priority_place::boolean as priority_place,
        levelling_up.levelling_up_place::boolean as levelling_up_place,
        wellbeing.happiness_mean as happiness_mean,
        wellbeing.worthwhile_mean as worthwhile_mean,
        wellbeing.anxiety_mean as anxiety_mean,
        wellbeing.life_satisfaction_mean as life_satisfaction_mean,
        participation_table.media_consumption,
        participation_table.any_library_engagement,
        participation_table.streamed_online_digital_events,
        participation_table.museums_and_galleries,
        participation_table.participating_in_creative_activities,
        participation_table.heritage,
        participation_table.attending_in_person_events,
        where_attended_culture.attended_events_local,
        where_attended_culture.participated_creative_activities_local,
        where_attended_culture.visited_culture_places_local,
        desire_for_creativity.desire_to_be_more_creative,
        feeling_creative.feeling_you_are_creative,
        expectation_to_attend.expectation_to_attend_events_next_12_months,
        expectation_to_attend.expectation_to_visit_places_next_12_months,
        expectation_to_attend.expectation_to_participate_in_activities_next_12_months,
        participation_table.all_creative_activities_and_media_consumption,
        participation_table.any_cultural_places_engagement,
        participation_table.attending_or_watching_cultural_events
    from wm_lads
    join population_table on wm_lads.lad22cd = population_table.code
    join estimated_populations on wm_lads.lad22cd = estimated_populations.lad23cd
    join households on wm_lads.lad22cd = households.lad22cd
    join community_life_metrics on wm_lads.lad22cd = community_life_metrics.lad23cd
    join priority_places on wm_lads.lad22cd = priority_places.lad22cd
    join levelling_up on wm_lads.lad22cd = levelling_up.lad22cd
    join wellbeing on wm_lads.lad22cd = wellbeing.area_code
    join participation_table on wm_lads.lad22cd = participation_table.lad23cd
    join where_attended_culture on wm_lads.lad22cd = where_attended_culture.lad22cd
    join desire_for_creativity on wm_lads.lad22cd = desire_for_creativity.lad22cd
    join feeling_creative on wm_lads.lad22cd = feeling_creative.lad22cd
    join expectation_to_attend on wm_lads.lad22cd = expectation_to_attend.lad22cd
)

select * from combined