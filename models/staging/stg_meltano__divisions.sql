select
    /* Primary Key */
    id

    /* Foreign Keys */
    , conference.id as conference_id

    /* Properties */
    , name as division_name
    , nameshort as division_name_short
    , link as division_url
    , abbreviation as division_abbreviation
    , active as is_active
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'divisions') }}
