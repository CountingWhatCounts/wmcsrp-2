with

distinct_services as (
    select
        distinct service_name,
        msoa21,
        service_type
    from {{ ref('int__cultural_infrastructure') }}
)

select
    msoa21,
    service_type,
    count(service_name) as service_count
from distinct_services
group by msoa21, service_type