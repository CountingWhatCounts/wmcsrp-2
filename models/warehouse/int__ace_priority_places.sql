with

priority_places as (
    select * from {{ ref('stg__ace_priority_places') }}
),

local_authority_codes as (
    select * from {{ ref('int__local_authority_codes') }}
),

combined as (
    select
        lad22cd,
        priority_places.lad22nm,
        priority_place
    from priority_places join local_authority_codes
    on priority_places.lad22nm = local_authority_codes.lad22nm
)

select * from combined