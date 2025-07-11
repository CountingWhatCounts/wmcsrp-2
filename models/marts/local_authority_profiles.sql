
with

profiles as (
    select
        lad22cd as area_code,
        lad22nm as area,
        population,
        number_of_households,
        priority_place,
        levelling_up_place,
        round(proud_to_live_locally::numeric, 2) as proud_to_live_locally,
        round(local_area_satisfaction::numeric, 2) as local_area_satisfaction,
        round(neighbourhood_belonging::numeric, 2) as neighbourhood_belonging,
        round(social_cohesion::numeric, 2) as social_cohesion,
        round(area_recommendation::numeric, 2) as area_recommendation,
        happiness_mean,
        worthwhile_mean,
        anxiety_mean,
        life_satisfaction_mean,
        round(attended_events_local::numeric, 5) as attended_events_local,
        round(participated_creative_activities_local::numeric, 5) as participated_creative_activities_local,
        round(visited_culture_places_local::numeric, 5) as visited_culture_places_local,
        round(desire_to_be_more_creative::numeric, 5) as desire_to_be_more_creative,
        round(feeling_you_are_creative::numeric, 5) as feeling_you_are_creative,
        round(media_consumption::numeric, 5) as media_consumption,
        round(any_library_engagement::numeric, 5) as any_library_engagement,
        round(streamed_online_digital_events::numeric, 5) as streamed_online_digital_events,
        round(museums_and_galleries::numeric, 5) as museums_and_galleries,
        round(participating_in_creative_activities::numeric, 5) as participating_in_creative_activities,
        round(heritage::numeric, 5) as heritage,
        round(attending_in_person_events::numeric, 5) as attending_in_person_events,
        round(all_creative_activities_and_media_consumption::numeric, 5) as all_creative_activities_and_media_consumption,
        round(any_cultural_places_engagement::numeric, 5) as any_cultural_places_engagement,
        round(attending_or_watching_cultural_events::numeric, 5) as attending_or_watching_cultural_events,
        round(expectation_to_attend_events_next_12_months::numeric, 5) as expectation_to_attend_events_next_12_months,
        round(expectation_to_participate_in_activities_next_12_months::numeric, 5) as expectation_to_participate_in_activities_next_12_months,
        round(expectation_to_visit_places_next_12_months::numeric, 5) as expectation_to_visit_places_next_12_months
    from {{ ref('int__local_authority_profiles') }}
),

calculated_benchmarks as (
    select
        NULL::text as area_code,
        group_label as area,
        population,
        number_of_households,
        NULL::boolean as priority_place,
        NULL::boolean as levelling_up_place,
        round(proud_to_live_locally::numeric, 2) as proud_to_live_locally,
        round(local_area_satisfaction::numeric, 2) as local_area_satisfaction,
        round(neighbourhood_belonging::numeric, 2) as neighbourhood_belonging,
        round(social_cohesion::numeric, 2) as social_cohesion,
        round(area_recommendation::numeric, 2) as area_recommendation,
        round(happiness_mean::numeric, 2) as happiness_mean,
        round(worthwhile_mean::numeric, 2) as worthwhile_mean,
        round(anxiety_mean::numeric, 2) as anxiety_mean,
        round(life_satisfaction_mean::numeric, 2) as life_satisfaction_mean,
        round(attended_events_local::numeric, 5) as attended_events_local,
        round(participated_creative_activities_local::numeric, 5) as participated_creative_activities_local,
        round(visited_culture_places_local::numeric, 5) as visited_culture_places_local,
        round(desire_to_be_more_creative::numeric, 5) as desire_to_be_more_creative,
        round(feeling_you_are_creative::numeric, 5) as feeling_you_are_creative,
        round(media_consumption::numeric, 5) as media_consumption,
        round(any_library_engagement::numeric, 5) as any_library_engagement,
        round(streamed_online_digital_events::numeric, 5) as streamed_online_digital_events,
        round(museums_and_galleries::numeric, 5) as museums_and_galleries,
        round(participating_in_creative_activities::numeric, 5) as participating_in_creative_activities,
        round(heritage::numeric, 5) as heritage,
        round(attending_in_person_events::numeric, 5) as attending_in_person_events,
        round(all_creative_activities_and_media_consumption::numeric, 5) as all_creative_activities_and_media_consumption,
        round(any_cultural_places_engagement::numeric, 5) as any_cultural_places_engagement,
        round(attending_or_watching_cultural_events::numeric, 5) as attending_or_watching_cultural_events,
        round(expectation_to_attend_events_next_12_months::numeric, 5) as expectation_to_attend_events_next_12_months,
        round(expectation_to_participate_in_activities_next_12_months::numeric, 5) as expectation_to_participate_in_activities_next_12_months,
        round(expectation_to_visit_places_next_12_months::numeric, 5) as expectation_to_visit_places_next_12_months
    from {{ ref('int__west_midlands_benchmarks') }}
),

imported_benchmarks as (
    select
        area_code,
        area,
        population,
        number_of_households,
        priority_place,
        levelling_up_place,
        round(proud_to_live_locally::numeric, 2) as proud_to_live_locally,
        round(local_area_satisfaction::numeric, 2) as local_area_satisfaction,
        round(neighbourhood_belonging::numeric, 2) as neighbourhood_belonging,
        round(social_cohesion::numeric, 2) as social_cohesion,
        round(area_recommendation::numeric, 2) as area_recommendation,
        happiness_mean,
        worthwhile_mean,
        anxiety_mean,
        life_satisfaction_mean,
        attended_events_local,
        participated_creative_activities_local,
        visited_culture_places_local,
        desire_to_be_more_creative,
        feeling_creative,
        media_consumption,
        any_library_engagement,
        streamed_online_digital_events,
        museums_and_galleries,
        participating_in_creative_activities,
        heritage,
        attending_in_person_events,
        all_creative_activities_and_media_consumption,
        any_cultural_places_engagement,
        attending_or_watching_cultural_events,
        expectation_to_attend_events_next_12_months,
        expectation_to_participate_in_activities_next_12_months,
        expectation_to_visit_places_next_12_months
    from {{ ref('int__england_benchmarks') }}
    where area = 'England'
),

combined as (
    select * from profiles
    union all
    select * from calculated_benchmarks
    union all
    select * from imported_benchmarks
),

dcms_stats as (
    select * from {{ ref('int__dcms_participation_lad_itl1_england') }}
),

combined_with_dcms as (
    select * from combined full outer join dcms_stats using (area)
)

select * from combined_with_dcms
order by array_position(array[
	'Herefordshire',
	'Stafford',
	'Lichfield',
	'Wyre Forest',
	'Stratford-on-Avon',
	'Wolverhampton',
	'Wychavon',
	'Coventry',
	'Shropshire',
	'Warwick',
	'Sandwell',
	'Walsall',
	'South Staffordshire',
	'Tamworth',
	'Bromsgrove',
	'Birmingham',
	'East Staffordshire',
	'Redditch',
	'Staffordshire Moorlands',
	'Telford and Wrekin',
	'North Warwickshire',
	'Nuneaton and Bedworth',
	'Rugby',
	'Malvern Hills',
	'Solihull',
	'Cannock Chase',
	'Dudley',
	'Worcester',
	'Newcastle-under-Lyme',
	'Stoke-on-Trent',
	'WMCA Constituent Members',
	'WMCA Non-Constituent Members',
	'WMCA Constituent and Non-Constituent Members',
	'West Midlands',
	'West Midlands Non-WMCA',
	'England',
	'East of England',
	'South East',
	'Yorkshire and The Humber',
	'East Midlands',
	'North West',
	'North East',
	'London',
	'South West'
], area)