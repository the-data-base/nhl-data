SELECT 
    /* Primary Key */
    conferences.id

    /* Properties */
    , conferences.name as conference_name
    , conferences.shortName as conference_short_name
    , conferences.abbreviation as conference_abbreviation
    , conferences.active as is_active
    , conferences.link as conference_url
FROM 
    {{ source('meltano', 'conferences') }} as conferences
