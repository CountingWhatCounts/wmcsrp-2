with

distinct_services as (
    select
        distinct service_name,
        msoa21cd,
        service_type
    from "wmcsrp2"."public_warehouse"."int__cultural_infrastructure"
),


services_count as (
    select
        msoa21cd,
        service_type,
        CAST(count(service_name) as integer) as service_count
    from distinct_services
    group by msoa21cd, service_type
),

remove_null as (
    select * from services_count
    where msoa21cd is not null
)

select * from remove_null