with

distinct_services as (
    select
        distinct service_name,
        lad22cd,
        service_type
    from "WMCSRP2"."main_warehouse"."int__cultural_infrastructure"
)

select
    lad22cd,
    service_type,
    count(service_name) as service_count
from distinct_services
group by lad22cd, service_type