select
  /* Primary Key */
  {{ dbt_utils.surrogate_key(['players.id', 'players.teamid', 'players.seasonid']) }} as stg_nhl__players_id

  /* Identifiers */
  , players.id as player_id
  , players.seasonid as season_id
  , players.teamid as team_id

  /* Properties */
  , players.fullname as full_name
  , players.link as player_url
  , players.firstname as first_name
  , players.lastname as last_name
  , players.primarynumber as primary_number
  , players.birthdate as birth_date
  , players.birthcity as birth_city
  , players.birthstateprovince as birth_state_province
  , players.birthcountry as birth_country
  , players.nationality as nationality
  , players.height
  , players.weight
  , players.active as is_active
  , players.alternatecaptain as is_alternate_captain
  , players.captain as is_captain
  , players.rookie as is_rookie
  , players.shootscatches as shoots_catches
  , players.rosterstatus as roster_status
  , players.primaryposition.code as primary_position_code
  , players.primaryposition.name as primary_position_name
  , players.primaryposition.type as primary_position_type
  , players.primaryposition.abbreviation as primary_position_abbreviation
  , players._time_extracted as extracted_at
  , players._time_loaded as loaded_at
from {{ source('meltano', 'people') }} as players
