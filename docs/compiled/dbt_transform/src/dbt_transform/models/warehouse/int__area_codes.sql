with

target_areas as (
    select
        case
            when lad22nm in (
                'Birmingham',
                'Coventry',
                'Dudley',
                'Sandwell',
                'Solihull',
                'Walsall',
                'Wolverhampton'
            ) then 'WMCA Constituent Member'
            when lad22nm in (
                'Cannock Chase',
                'North Warwickshire',
                'Nuneaton and Bedworth',
                'Redditch',
                'Rugby',
                'Shropshire',
                'Stratford-on-Avon',
                'Tamworth',
                'Telford and Wrekin',
                'Warwick'
            ) then 'WMCA Non-Constituent Member'
            when lad22nm in (
                'Bromsgrove',
                'East Staffordshire',
                'Herefordshire, County of',
                'Lichfield',
                'Malvern Hills',
                'Newcastle-under-Lyme',
                'South Staffordshire',
                'Stafford',
                'Staffordshire Moorlands',
                'Stoke-on-Trent',
                'Worcester',
                'Wychavon',
                'Wyre Forest'
            ) then 'West Midlands Non-WMCA'
            else 'Ignore'
        end as area,
        lad22cd,
        lad22nm,
        msoa21cd,
        msoa21nm
    from
        "dev"."main_staging"."stg__msoa_mapping"
)

select * from target_areas where area != 'Ignore'