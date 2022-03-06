SELECT 
    /* Primary Key */
    d.id

    /* Foreign Keys */
    ,d.conference.id as conference_id

    /* Properties */
    ,d.name as division_name
    ,d.nameshort as division_name_short
    ,d.link as division_url
    ,d.abbreviation as division_abbreviation
    ,d.active as is_active

FROM 
    {{ ref('stg_meltano__divisions') }}
