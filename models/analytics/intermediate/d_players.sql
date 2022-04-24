select
    /* Primary Key */
    players.id
    /* Foreign Keys */
    , players.current_team_id
    /* Player Properties */
    , players.full_name
    , players.player_url
    , players.first_name
    , players.last_name
    , players.primary_number
    , players.birth_date
    , players.current_age
    , players.birth_city
    , players.birth_state_province
    , players.birth_country
    , players.nationality
    , players.height
    , players.weight
    , players.is_active
    , players.is_alternate_captain
    , players.is_captain
    , players.is_rookie
    , players.shoots_catches
    , players.roster_status
    , players.current_team_name
    , players.current_team_url
    , players.primary_position_code
    , players.primary_position_name
    , players.primary_position_type
    , players.primary_position_abbreviation
    , players.extracted_at
    , players.loaded_at
from 
    {{ ref('stg_nhl__players') }} as players

