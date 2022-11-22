select distinct
    /* Primary Key */
    stg_nhl__draft_prospects_id as draft_prospects_id

    /* Identifiers */
    , draft_prospect_id
    , prospect_player_id
    , prospect_category_id

    /* Properties */
    , prospect_first_name
    , prospect_last_name
    , prospect_full_name
    , prospect_birth_date
    , prospect_age_years
    , prospect_age_days
    , prospect_birth_city
    , prospect_birth_state_province
    , prospect_birth_country
    , prospect_height
    , prospect_weight
    , prospect_shoots_catches
    , prospect_position_name
    , prospect_position_abbreviation
    , prospect_draft_status -- wtf is this?
    , prospect_category_name
    , prospect_category_short_name
    , prospect_amateur_team_name
    , prospect_amateur_team_url
    , prospect_amateur_league_name
    , prospect_amateur_league_url
    , prospect_rank_midterm
    , prospect_rank_draft_year
    , prospect_url
from {{ ref('stg_nhl__draft_prospects') }}

