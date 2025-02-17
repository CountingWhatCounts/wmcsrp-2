select
    msoa21cd,
    service_type,
    service_count
from {{ ref('int__cultural_infrastructure_msoa') }}