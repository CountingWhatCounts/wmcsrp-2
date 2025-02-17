select
    CAST(local_authority as text) as local_authority,
    CAST(measure as text) as measure,
    CAST(value as float) as value,
    CAST(margin_of_error as float) as margin_of_error
from
    {{ source('preprocessed_data', 'raw__economic') }}