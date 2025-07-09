with

local_authority_profiles as (
    select * from "wmcsrp2"."public_warehouse"."int__local_authority_profiles"
),

aggregated as (

-- WMCA Constituent Members


select
        'WMCA Constituent Members' as group_label,
        SUM(population) as population,
        SUM(number_of_households) as number_of_households,
        SUM(estimated_age_16_plus_population_count * proud_to_live_locally) / SUM(estimated_age_16_plus_population_count) as proud_to_live_locally,
        SUM(estimated_age_16_plus_population_count * local_area_satisfaction) / SUM(estimated_age_16_plus_population_count) as local_area_satisfaction,
        SUM(estimated_age_16_plus_population_count * neighbourhood_belonging) / SUM(estimated_age_16_plus_population_count) as neighbourhood_belonging,
        SUM(estimated_age_16_plus_population_count * social_cohesion) / SUM(estimated_age_16_plus_population_count) as social_cohesion,
        SUM(estimated_age_16_plus_population_count * area_recommendation) / SUM(estimated_age_16_plus_population_count) as area_recommendation,
        SUM(estimated_age_16_plus_population_count * happiness_mean) / SUM(estimated_age_16_plus_population_count) as happiness_mean,
        SUM(estimated_age_16_plus_population_count * worthwhile_mean) / SUM(estimated_age_16_plus_population_count) as worthwhile_mean,
        SUM(estimated_age_16_plus_population_count * anxiety_mean) / SUM(estimated_age_16_plus_population_count) as anxiety_mean,
        SUM(estimated_age_16_plus_population_count * life_satisfaction_mean) / SUM(estimated_age_16_plus_population_count) as life_satisfaction_mean,
        SUM(estimated_age_16_plus_population_count * media_consumption) / SUM(estimated_age_16_plus_population_count) as media_consumption,
        SUM(estimated_age_16_plus_population_count * any_library_engagement) / SUM(estimated_age_16_plus_population_count) as any_library_engagement,
        SUM(estimated_age_16_plus_population_count * streamed_online_digital_events) / SUM(estimated_age_16_plus_population_count) as streamed_online_digital_events,
        SUM(estimated_age_16_plus_population_count * museums_and_galleries) / SUM(estimated_age_16_plus_population_count) as museums_and_galleries,
        SUM(estimated_age_16_plus_population_count * participating_in_creative_activities) / SUM(estimated_age_16_plus_population_count) as participating_in_creative_activities,
        SUM(estimated_age_16_plus_population_count * heritage) / SUM(estimated_age_16_plus_population_count) as heritage,
        SUM(estimated_age_16_plus_population_count * attending_in_person_events) / SUM(estimated_age_16_plus_population_count) as attending_in_person_events,
        SUM(estimated_age_16_plus_population_count * attended_events_local) / SUM(estimated_age_16_plus_population_count) as attended_events_local,
        SUM(estimated_age_16_plus_population_count * participated_creative_activities_local) / SUM(estimated_age_16_plus_population_count) as participated_creative_activities_local,
        SUM(estimated_age_16_plus_population_count * visited_culture_places_local) / SUM(estimated_age_16_plus_population_count) as visited_culture_places_local,
        SUM(estimated_age_16_plus_population_count * desire_to_be_more_creative) / SUM(estimated_age_16_plus_population_count) as desire_to_be_more_creative,
        SUM(estimated_age_16_plus_population_count * feeling_you_are_creative) / SUM(estimated_age_16_plus_population_count) as feeling_you_are_creative,
        SUM(estimated_age_16_plus_population_count * all_creative_activities_and_media_consumption) / SUM(estimated_age_16_plus_population_count) as all_creative_activities_and_media_consumption,
        SUM(estimated_age_16_plus_population_count * any_cultural_places_engagement) / SUM(estimated_age_16_plus_population_count) as any_cultural_places_engagement,
        SUM(estimated_age_16_plus_population_count * attending_or_watching_cultural_events) / SUM(estimated_age_16_plus_population_count) as attending_or_watching_cultural_events,
        SUM(estimated_age_16_plus_population_count * expectation_to_attend_events_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_attend_events_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_visit_places_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_visit_places_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_participate_in_activities_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_participate_in_activities_next_12_months
    from local_authority_profiles
    where in_wmca = 'WMCA Constituent Member'

    union all

    select
        'WMCA Non-Constituent Members' as group_label,
        SUM(population) as population,
        SUM(number_of_households) as number_of_households,
        SUM(estimated_age_16_plus_population_count * proud_to_live_locally) / SUM(estimated_age_16_plus_population_count) as proud_to_live_locally,
        SUM(estimated_age_16_plus_population_count * local_area_satisfaction) / SUM(estimated_age_16_plus_population_count) as local_area_satisfaction,
        SUM(estimated_age_16_plus_population_count * neighbourhood_belonging) / SUM(estimated_age_16_plus_population_count) as neighbourhood_belonging,
        SUM(estimated_age_16_plus_population_count * social_cohesion) / SUM(estimated_age_16_plus_population_count) as social_cohesion,
        SUM(estimated_age_16_plus_population_count * area_recommendation) / SUM(estimated_age_16_plus_population_count) as area_recommendation,
        SUM(estimated_age_16_plus_population_count * happiness_mean) / SUM(estimated_age_16_plus_population_count) as happiness_mean,
        SUM(estimated_age_16_plus_population_count * worthwhile_mean) / SUM(estimated_age_16_plus_population_count) as worthwhile_mean,
        SUM(estimated_age_16_plus_population_count * anxiety_mean) / SUM(estimated_age_16_plus_population_count) as anxiety_mean,
        SUM(estimated_age_16_plus_population_count * life_satisfaction_mean) / SUM(estimated_age_16_plus_population_count) as life_satisfaction_mean,
        SUM(estimated_age_16_plus_population_count * media_consumption) / SUM(estimated_age_16_plus_population_count) as media_consumption,
        SUM(estimated_age_16_plus_population_count * any_library_engagement) / SUM(estimated_age_16_plus_population_count) as any_library_engagement,
        SUM(estimated_age_16_plus_population_count * streamed_online_digital_events) / SUM(estimated_age_16_plus_population_count) as streamed_online_digital_events,
        SUM(estimated_age_16_plus_population_count * museums_and_galleries) / SUM(estimated_age_16_plus_population_count) as museums_and_galleries,
        SUM(estimated_age_16_plus_population_count * participating_in_creative_activities) / SUM(estimated_age_16_plus_population_count) as participating_in_creative_activities,
        SUM(estimated_age_16_plus_population_count * heritage) / SUM(estimated_age_16_plus_population_count) as heritage,
        SUM(estimated_age_16_plus_population_count * attending_in_person_events) / SUM(estimated_age_16_plus_population_count) as attending_in_person_events,
        SUM(estimated_age_16_plus_population_count * attended_events_local) / SUM(estimated_age_16_plus_population_count) as attended_events_local,
        SUM(estimated_age_16_plus_population_count * participated_creative_activities_local) / SUM(estimated_age_16_plus_population_count) as participated_creative_activities_local,
        SUM(estimated_age_16_plus_population_count * visited_culture_places_local) / SUM(estimated_age_16_plus_population_count) as visited_culture_places_local,
        SUM(estimated_age_16_plus_population_count * desire_to_be_more_creative) / SUM(estimated_age_16_plus_population_count) as desire_to_be_more_creative,
        SUM(estimated_age_16_plus_population_count * feeling_you_are_creative) / SUM(estimated_age_16_plus_population_count) as feeling_you_are_creative,
        SUM(estimated_age_16_plus_population_count * all_creative_activities_and_media_consumption) / SUM(estimated_age_16_plus_population_count) as all_creative_activities_and_media_consumption,
        SUM(estimated_age_16_plus_population_count * any_cultural_places_engagement) / SUM(estimated_age_16_plus_population_count) as any_cultural_places_engagement,
        SUM(estimated_age_16_plus_population_count * attending_or_watching_cultural_events) / SUM(estimated_age_16_plus_population_count) as attending_or_watching_cultural_events,
        SUM(estimated_age_16_plus_population_count * expectation_to_attend_events_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_attend_events_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_visit_places_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_visit_places_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_participate_in_activities_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_participate_in_activities_next_12_months
    from local_authority_profiles
    where in_wmca = 'WMCA Non-Constituent Member'

    union all

    select
        'WMCA Constituent and Non-Constituent Members' as group_label,
        SUM(population) as population,
        SUM(number_of_households) as number_of_households,
        SUM(estimated_age_16_plus_population_count::float * proud_to_live_locally) / SUM(estimated_age_16_plus_population_count::float) as proud_to_live_locally,
        SUM(estimated_age_16_plus_population_count::float * local_area_satisfaction) / SUM(estimated_age_16_plus_population_count::float) as local_area_satisfaction,
        SUM(estimated_age_16_plus_population_count::float * neighbourhood_belonging) / SUM(estimated_age_16_plus_population_count::float) as neighbourhood_belonging,
        SUM(estimated_age_16_plus_population_count::float * social_cohesion) / SUM(estimated_age_16_plus_population_count::float) as social_cohesion,
        SUM(estimated_age_16_plus_population_count::float * area_recommendation) / SUM(estimated_age_16_plus_population_count::float) as area_recommendation,
        SUM(estimated_age_16_plus_population_count::float * happiness_mean) / SUM(estimated_age_16_plus_population_count::float) as happiness_mean,
        SUM(estimated_age_16_plus_population_count::float * worthwhile_mean) / SUM(estimated_age_16_plus_population_count::float) as worthwhile_mean,
        SUM(estimated_age_16_plus_population_count::float * anxiety_mean) / SUM(estimated_age_16_plus_population_count::float) as anxiety_mean,
        SUM(estimated_age_16_plus_population_count::float * life_satisfaction_mean) / SUM(estimated_age_16_plus_population_count::float) as life_satisfaction_mean,
        SUM(estimated_age_16_plus_population_count::float * media_consumption) / SUM(estimated_age_16_plus_population_count::float) as media_consumption,
        SUM(estimated_age_16_plus_population_count::float * any_library_engagement) / SUM(estimated_age_16_plus_population_count::float) as any_library_engagement,
        SUM(estimated_age_16_plus_population_count::float * streamed_online_digital_events) / SUM(estimated_age_16_plus_population_count::float) as streamed_online_digital_events,
        SUM(estimated_age_16_plus_population_count::float * museums_and_galleries) / SUM(estimated_age_16_plus_population_count::float) as museums_and_galleries,
        SUM(estimated_age_16_plus_population_count::float * participating_in_creative_activities) / SUM(estimated_age_16_plus_population_count::float) as participating_in_creative_activities,
        SUM(estimated_age_16_plus_population_count::float * heritage) / SUM(estimated_age_16_plus_population_count::float) as heritage,
        SUM(estimated_age_16_plus_population_count::float * attending_in_person_events) / SUM(estimated_age_16_plus_population_count::float) as attending_in_person_events,
        SUM(estimated_age_16_plus_population_count::float * attended_events_local) / SUM(estimated_age_16_plus_population_count::float) as attended_events_local,
        SUM(estimated_age_16_plus_population_count::float * participated_creative_activities_local) / SUM(estimated_age_16_plus_population_count::float) as participated_creative_activities_local,
        SUM(estimated_age_16_plus_population_count::float * visited_culture_places_local) / SUM(estimated_age_16_plus_population_count::float) as visited_culture_places_local,
        SUM(estimated_age_16_plus_population_count::float * desire_to_be_more_creative) / SUM(estimated_age_16_plus_population_count::float) as desire_to_be_more_creative,
        SUM(estimated_age_16_plus_population_count::float * feeling_you_are_creative) / SUM(estimated_age_16_plus_population_count::float) as feeling_you_are_creative,
        SUM(estimated_age_16_plus_population_count::float * all_creative_activities_and_media_consumption) / SUM(estimated_age_16_plus_population_count::float) as all_creative_activities_and_media_consumption,
        SUM(estimated_age_16_plus_population_count::float * any_cultural_places_engagement) / SUM(estimated_age_16_plus_population_count::float) as any_cultural_places_engagement,
        SUM(estimated_age_16_plus_population_count::float * attending_or_watching_cultural_events) / SUM(estimated_age_16_plus_population_count::float) as attending_or_watching_cultural_events,
        SUM(estimated_age_16_plus_population_count * expectation_to_attend_events_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_attend_events_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_visit_places_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_visit_places_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_participate_in_activities_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_participate_in_activities_next_12_months
    from local_authority_profiles
    where in_wmca in ('WMCA Constituent Member', 'WMCA Non-Constituent Member')

    union all

    select
        'West Midlands Non-WMCA' as group_label,
        SUM(population) as population,
        SUM(number_of_households) as number_of_households,
        SUM(estimated_age_16_plus_population_count::float * proud_to_live_locally) / SUM(estimated_age_16_plus_population_count::float) as proud_to_live_locally,
        SUM(estimated_age_16_plus_population_count::float * local_area_satisfaction) / SUM(estimated_age_16_plus_population_count::float) as local_area_satisfaction,
        SUM(estimated_age_16_plus_population_count::float * neighbourhood_belonging) / SUM(estimated_age_16_plus_population_count::float) as neighbourhood_belonging,
        SUM(estimated_age_16_plus_population_count::float * social_cohesion) / SUM(estimated_age_16_plus_population_count::float) as social_cohesion,
        SUM(estimated_age_16_plus_population_count::float * area_recommendation) / SUM(estimated_age_16_plus_population_count::float) as area_recommendation,
        SUM(estimated_age_16_plus_population_count::float * happiness_mean) / SUM(estimated_age_16_plus_population_count::float) as happiness_mean,
        SUM(estimated_age_16_plus_population_count::float * worthwhile_mean) / SUM(estimated_age_16_plus_population_count::float) as worthwhile_mean,
        SUM(estimated_age_16_plus_population_count::float * anxiety_mean) / SUM(estimated_age_16_plus_population_count::float) as anxiety_mean,
        SUM(estimated_age_16_plus_population_count::float * life_satisfaction_mean) / SUM(estimated_age_16_plus_population_count::float) as life_satisfaction_mean,
        SUM(estimated_age_16_plus_population_count::float * media_consumption) / SUM(estimated_age_16_plus_population_count::float) as media_consumption,
        SUM(estimated_age_16_plus_population_count::float * any_library_engagement) / SUM(estimated_age_16_plus_population_count::float) as any_library_engagement,
        SUM(estimated_age_16_plus_population_count::float * streamed_online_digital_events) / SUM(estimated_age_16_plus_population_count::float) as streamed_online_digital_events,
        SUM(estimated_age_16_plus_population_count::float * museums_and_galleries) / SUM(estimated_age_16_plus_population_count::float) as museums_and_galleries,
        SUM(estimated_age_16_plus_population_count::float * participating_in_creative_activities) / SUM(estimated_age_16_plus_population_count::float) as participating_in_creative_activities,
        SUM(estimated_age_16_plus_population_count::float * heritage) / SUM(estimated_age_16_plus_population_count::float) as heritage,
        SUM(estimated_age_16_plus_population_count::float * attending_in_person_events) / SUM(estimated_age_16_plus_population_count::float) as attending_in_person_events,
        SUM(estimated_age_16_plus_population_count::float * attended_events_local) / SUM(estimated_age_16_plus_population_count::float) as attended_events_local,
        SUM(estimated_age_16_plus_population_count::float * participated_creative_activities_local) / SUM(estimated_age_16_plus_population_count::float) as participated_creative_activities_local,
        SUM(estimated_age_16_plus_population_count::float * visited_culture_places_local) / SUM(estimated_age_16_plus_population_count::float) as visited_culture_places_local,
        SUM(estimated_age_16_plus_population_count::float * desire_to_be_more_creative) / SUM(estimated_age_16_plus_population_count::float) as desire_to_be_more_creative,
        SUM(estimated_age_16_plus_population_count::float * feeling_you_are_creative) / SUM(estimated_age_16_plus_population_count::float) as feeling_you_are_creative,
        SUM(estimated_age_16_plus_population_count::float * all_creative_activities_and_media_consumption) / SUM(estimated_age_16_plus_population_count::float) as all_creative_activities_and_media_consumption,
        SUM(estimated_age_16_plus_population_count::float * any_cultural_places_engagement) / SUM(estimated_age_16_plus_population_count::float) as any_cultural_places_engagement,
        SUM(estimated_age_16_plus_population_count::float * attending_or_watching_cultural_events) / SUM(estimated_age_16_plus_population_count::float) as attending_or_watching_cultural_events,
        SUM(estimated_age_16_plus_population_count * expectation_to_attend_events_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_attend_events_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_visit_places_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_visit_places_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_participate_in_activities_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_participate_in_activities_next_12_months
    from local_authority_profiles
    where in_wmca = 'West Midlands Non-WMCA'

    union all

    select
        'West Midlands' as group_label,
        SUM(population) as population,
        SUM(number_of_households) as number_of_households,
        SUM(estimated_age_16_plus_population_count::float * proud_to_live_locally) / SUM(estimated_age_16_plus_population_count::float) as proud_to_live_locally,
        SUM(estimated_age_16_plus_population_count::float * local_area_satisfaction) / SUM(estimated_age_16_plus_population_count::float) as local_area_satisfaction,
        SUM(estimated_age_16_plus_population_count::float * neighbourhood_belonging) / SUM(estimated_age_16_plus_population_count::float) as neighbourhood_belonging,
        SUM(estimated_age_16_plus_population_count::float * social_cohesion) / SUM(estimated_age_16_plus_population_count::float) as social_cohesion,
        SUM(estimated_age_16_plus_population_count::float * area_recommendation) / SUM(estimated_age_16_plus_population_count::float) as area_recommendation,
        SUM(estimated_age_16_plus_population_count::float * happiness_mean) / SUM(estimated_age_16_plus_population_count::float) as happiness_mean,
        SUM(estimated_age_16_plus_population_count::float * worthwhile_mean) / SUM(estimated_age_16_plus_population_count::float) as worthwhile_mean,
        SUM(estimated_age_16_plus_population_count::float * anxiety_mean) / SUM(estimated_age_16_plus_population_count::float) as anxiety_mean,
        SUM(estimated_age_16_plus_population_count::float * life_satisfaction_mean) / SUM(estimated_age_16_plus_population_count::float) as life_satisfaction_mean,
        SUM(estimated_age_16_plus_population_count::float * media_consumption) / SUM(estimated_age_16_plus_population_count::float) as media_consumption,
        SUM(estimated_age_16_plus_population_count::float * any_library_engagement) / SUM(estimated_age_16_plus_population_count::float) as any_library_engagement,
        SUM(estimated_age_16_plus_population_count::float * streamed_online_digital_events) / SUM(estimated_age_16_plus_population_count::float) as streamed_online_digital_events,
        SUM(estimated_age_16_plus_population_count::float * museums_and_galleries) / SUM(estimated_age_16_plus_population_count::float) as museums_and_galleries,
        SUM(estimated_age_16_plus_population_count::float * participating_in_creative_activities) / SUM(estimated_age_16_plus_population_count::float) as participating_in_creative_activities,
        SUM(estimated_age_16_plus_population_count::float * heritage) / SUM(estimated_age_16_plus_population_count::float) as heritage,
        SUM(estimated_age_16_plus_population_count::float * attending_in_person_events) / SUM(estimated_age_16_plus_population_count::float) as attending_in_person_events,
        SUM(estimated_age_16_plus_population_count::float * attended_events_local) / SUM(estimated_age_16_plus_population_count::float) as attended_events_local,
        SUM(estimated_age_16_plus_population_count::float * participated_creative_activities_local) / SUM(estimated_age_16_plus_population_count::float) as participated_creative_activities_local,
        SUM(estimated_age_16_plus_population_count::float * visited_culture_places_local) / SUM(estimated_age_16_plus_population_count::float) as visited_culture_places_local,
        SUM(estimated_age_16_plus_population_count::float * desire_to_be_more_creative) / SUM(estimated_age_16_plus_population_count::float) as desire_to_be_more_creative,
        SUM(estimated_age_16_plus_population_count::float * feeling_you_are_creative) / SUM(estimated_age_16_plus_population_count::float) as feeling_you_are_creative,
        SUM(estimated_age_16_plus_population_count::float * all_creative_activities_and_media_consumption) / SUM(estimated_age_16_plus_population_count::float) as all_creative_activities_and_media_consumption,
        SUM(estimated_age_16_plus_population_count::float * any_cultural_places_engagement) / SUM(estimated_age_16_plus_population_count::float) as any_cultural_places_engagement,
        SUM(estimated_age_16_plus_population_count::float * attending_or_watching_cultural_events) / SUM(estimated_age_16_plus_population_count::float) as attending_or_watching_cultural_events,
        SUM(estimated_age_16_plus_population_count * expectation_to_attend_events_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_attend_events_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_visit_places_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_visit_places_next_12_months,
        SUM(estimated_age_16_plus_population_count * expectation_to_participate_in_activities_next_12_months) / SUM(estimated_age_16_plus_population_count) as expectation_to_participate_in_activities_next_12_months
    from local_authority_profiles

    

)

select * from aggregated