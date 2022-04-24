select
    /* Primary Key */
    conferences.id as id

    /* Properties */
    , conferences.name as conference_name
    , conferences.shortname as conference_short_name
    , conferences.abbreviation as conference_abbreviation
    , conferences.active as is_active
    , conferences.link as conference_url
from {{ source('meltano', 'conferences') }} as conferences
