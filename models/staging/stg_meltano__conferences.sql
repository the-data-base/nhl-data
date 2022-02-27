select
    /* Primary Key */
    id

    /* Properties */
    , name as conference_name
    , link as conference_url
    , abbreviation as conference_abbreviation
    , shortName as conference_short_name
    , active as is_active
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'conferences') }}
