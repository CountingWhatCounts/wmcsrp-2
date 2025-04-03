select
    CAST("LAD23CD" as text) as lad23cd,
    CAST(participation_type as text) as participation_type,
    CAST(demographic as text) as demographic,
    CAST(value as float) as value
from
    {{ source('preprocessed_data', 'raw__modelled_participation_statistics') }}