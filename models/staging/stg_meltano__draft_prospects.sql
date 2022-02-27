select
    /* Primary Key */
    id

    /* Foreign Keys */
    , nhlPlayerId as nhl_player_id
    , prospectCategory.id as prospect_category_id

    /* Properties */
    , fullName as full_name
    , link as url
    , firstName as first_name
    , lastName as last_name
    , birthDate as birth_date
    , birthCity as birth_city
    , birthStateProvince as birth_state_province
    , birthCountry as birth_country
    , height
    , weight
    , shootsCatches as shoots_catches
    , primaryPosition as primary_position
    , primaryPosition.code as primary_position_code
    , primaryPosition.name as primary_position_name
    , primaryPosition.type as primary_position_type
    , primaryPosition.abbreviation as primary_position_abbreviation
    , draftStatus
    , prospectCategory.shortName as prospect_category_short_name
    , prospectCategory.name as prospect_category_name
    , amateurTeam as amateur_team
    , amateurTeam.name as amateur_team_name
    , amateurTeam.link as amateur_team_url
    , amateurLeague.name as amateur_league_name
    , amateurLeague.link as amateur_league_url
    , ranks.midterm as ranks_midterm
    , ranks.draftYear as ranks_draft_year
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'draft_prospects') }}
