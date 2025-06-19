with

itl as (
	SELECT participation_type, response_group, response_breakdown, percentage_of_respondents_2023_24, number_of_respondents_2023_24
	FROM "wmcsrp2"."public_warehouse"."int__dcms_participation_statistics"
	where
		participation_type in (
			'Attended Cultural Events in Person',
			'Participated in Creative Activities',
			'Visited a heritage site in person at least once in the last 12 months',
			'Visited a museum or gallery in person in the last 12 months',
			'Visited a public library building or mobile library in person in the last 12 months'
		)
		and response_group in ('Region (ITL1 level)')
),

lad as (
	SELECT participation_type, response_group, response_breakdown, percentage_of_respondents_2023_24, number_of_respondents_2023_24
	FROM "wmcsrp2"."public_warehouse"."int__dcms_participation_statistics"
	where
		participation_type in (
			'Attended Cultural Events in Person',
			'Participated in Creative Activities',
			'Visited a heritage site in person at least once in the last 12 months',
			'Visited a museum or gallery in person in the last 12 months',
			'Visited a public library building or mobile library in person in the last 12 months'
		)
		and response_group in ('Local authority district')
		and response_breakdown in (
			'Herefordshire, County of',
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
			'Stoke-on-Trent'
		)
),

total as (
	SELECT participation_type, response_group,'England' as response_breakdown, percentage_of_respondents_2023_24, number_of_respondents_2023_24
	FROM "wmcsrp2"."public_warehouse"."int__dcms_participation_statistics"
	where
		participation_type in (
			'Attended Cultural Events in Person',
			'Participated in Creative Activities',
			'Visited a heritage site in person at least once in the last 12 months',
			'Visited a museum or gallery in person in the last 12 months',
			'Visited a public library building or mobile library in person in the last 12 months'
		)
		and response_group in ('Total')
),

combined as (
	select * from itl
	union all
	select * from lad
	union all
	select * from total
)

select
    CASE
        WHEN response_breakdown = 'Herefordshire, County of' THEN 'Herefordshire'
        ELSE response_breakdown
    END as area,
    MAX(percentage_of_respondents_2023_24) FILTER (WHERE participation_type = 'Attended Cultural Events in Person') AS dcms_attended_cultural_events,
    MAX(percentage_of_respondents_2023_24) FILTER (WHERE participation_type = 'Participated in Creative Activities') AS dcms_participated_creative_activities,
    MAX(percentage_of_respondents_2023_24) FILTER (WHERE participation_type = 'Visited a heritage site in person at least once in the last 12 months') AS dcms_visited_heritage_site,
    MAX(percentage_of_respondents_2023_24) FILTER (WHERE participation_type = 'Visited a museum or gallery in person in the last 12 months') AS dcms_visited_museum_or_gallery,
    MAX(percentage_of_respondents_2023_24) FILTER (WHERE participation_type = 'Visited a public library building or mobile library in person in the last 12 months') AS dcms_visited_library
from combined
group by response_breakdown
order by response_breakdown