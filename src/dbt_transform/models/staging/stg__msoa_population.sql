select
    msoa_2021_code as msoa21cd,
    total as population
from
    {{ ref('seed_msoa_population') }}