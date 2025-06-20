with

community_life_benchmarks as (
    select
        benchmark,

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

    from "wmcsrp2"."public_marts"."community_life_survey_benchmarks"

    where metric IN (
        'How much do you agree or disagree with the following statements? - I am proud to live in my local area : Agree',
        'Overall, how satisfied or dissatisfied are you with your local area as a place to live? : Satisfied',
        'How strongly do you feel you belong to your immediate neighbourhood?: Total',
        'To what extent do you agree or disagree that this local area is a place where people from different backgrounds get on well together?: Agree',
        'How much do you agree or disagree with the following statements? - I would recommend my local area to others as a good place to live: Agree'
    )

    group by benchmark
),

wellbeing_benchmarks as (
    select
        case
			when area_code = 'E12000005' then 'West Midlands'
			when area_code = 'E92000001' then 'England'
			else area_code
		end as area_code,
		
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
    where area_code in ('E92000001', 'E12000005')

    group by area_code
),

benchmarks AS (

    SELECT
        /* --- identity columns --- */
        NULL::text AS area_code,
        clb.benchmark::text AS area,

/* --- things the benchmark has no data for --- */
NULL::bigint AS population,
        NULL::bigint AS number_of_households,

/* --- community-life metrics --- */


clb.proud_to_live_locally::float as proud_to_live_locally,
        clb.local_area_satisfaction::float as local_area_satisfaction,
        clb.neighbourhood_belonging::float as neighbourhood_belonging,
        clb.social_cohesion::float as social_cohesion,
        clb.area_recommendation::float as area_recommendation,

        NULL::boolean AS priority_place,
        NULL::boolean AS levelling_up_place,
        /* --- wellbeing metrics --- */
        wb.happiness_mean::float as happiness_mean,
        wb.worthwhile_mean::float as worthwhile_mean,
        wb.anxiety_mean::float as anxiety_mean,
        wb.life_satisfaction_mean::float as life_satisfaction_mean,

/* --- participation & resident-survey columns you don’t have here --- */

NULL::float as media_consumption,
        NULL::float as any_library_engagement,
        NULL::float as streamed_online_digital_events,
        NULL::float as museums_and_galleries,
        NULL::float as participating_in_creative_activities,
        NULL::float as heritage,
        NULL::float as attending_in_person_events,
        NULL::float as attended_events_local,
        NULL::float as participated_creative_activities_local,
        NULL::float as visited_culture_places_local,
        NULL::float as desire_to_be_more_creative,
        NULL::float as feeling_creative,
        NULL::float as all_creative_activities_and_media_consumption,
        NULL::float as any_cultural_places_engagement,
        NULL::float as attending_or_watching_cultural_events

    FROM community_life_benchmarks AS clb
    LEFT JOIN wellbeing_benchmarks AS wb ON wb.area_code = clb.benchmark
)

select * from benchmarks