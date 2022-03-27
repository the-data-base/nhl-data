select
    /* Primary Key */
    stg_meltano__divisions.id
    /* Foreign Keys */
    ,stg_meltano__divisions.conference_id
    /* Properties */
    ,stg_meltano__divisions.division_name
    ,stg_meltano__divisions.division_short_name 
    ,stg_meltano__divisions.division_url
    ,stg_meltano__divisions.division_abbreviation
    ,stg_meltano__divisions.is_active

from 
    {{ ref('stg_meltano__divisions') }}
