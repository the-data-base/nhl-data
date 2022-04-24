SELECT
    /* Primary Key */
    divisions.id

    /* Foreign Keys */
    ,divisions.conference.id as conference_id

    /* Properties */
    ,divisions.name as division_name
    ,divisions.nameshort as division_short_name
    ,divisions.link as division_url
    ,divisions.abbreviation as division_abbreviation
    ,divisions.active as is_active

FROM
    {{ source('meltano', 'divisions') }} as divisions
