select
    /* Primary Key */
    id

    /* Foreign Keys */
    , currentTeam.id as current_team_id

    /* Properties */
    , players.fullName as full_name
    , players.link as player_url
    , players.firstName as first_name
    , players.lastName as last_name
    , players.primaryNumber as primary_number
    , players.birthDate as birth_date
    , players.currentAge as current_age
    , players.birthCity as birth_city
    , players.birthStateProvince as birth_state_province
    , players.birthCountry as birth_country
    , players.nationality as nationality
    , players.height
    , players.weight
    , players.active as is_active
    , players.alternateCaptain as is_alternate_captain
    , players.captain as is_captain
    , players.rookie as is_rookie
    , players.shootsCatches as shoots_catches
    , players.rosterStatus as roster_status
    , players.currentTeam.name as current_team_name
    , players.currentTeam.link as current_team_url
    , players.primaryPosition.code as primary_position_code
    , players.primaryPosition.name as primary_position_name
    , players.primaryPosition.type as primary_position_type
    , players.primaryPosition.abbreviation as primary_position_abbreviation
    , players._time_extracted as extracted_at
    , players._time_loaded as loaded_at
from {{ source('meltano', 'players') }} as players
