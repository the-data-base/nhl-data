select
    /* Primary Key */
    id

    /* Foreign Keys */
    , currentTeam.id as current_team_id

    /* Properties */
    , fullName as full_name
    , link as player_url
    , firstName as first_name
    , lastName as last_name
    , primaryNumber as primary_number
    , birthDate as birth_date
    , currentAge as current_age
    , birthCity as birth_city
    , birthStateProvince as birth_state_province
    , birthCountry as birth_country
    , nationality as nationality
    , height
    , weight
    , active as is_active
    , alternateCaptain as is_alternate_captain
    , captain as is_captain
    , rookie as is_rookie
    , shootsCatches as shoots_catches
    , rosterStatus as roster_status
    , currentTeam.name as current_team_name
    , currentTeam.link as current_team_url
    , primaryPosition.code as primary_position_code
    , primaryPosition.name as primary_position_name
    , primaryPosition.type as primary_position_type
    , primaryPosition.abbreviation as primary_position_abbreviation
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'players') }}
