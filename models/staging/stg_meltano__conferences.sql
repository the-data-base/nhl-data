SELECT 
    /* Primary Key */
    c.id

    /* Properties */
    , c.name as conference_name
    , c.shortName as conference_short_name
    , c.abbreviation as conference_abbreviation
    , c.active as is_active
    , c.link as conference_url
FROM 
    {{ source('meltano', 'conferences') }}
