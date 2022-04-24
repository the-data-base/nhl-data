select distinct
    /* Primary Key */
    prospects.id
    /* Foreign Keys */
    , prospects.prospect_player_id
    , prospects.prospect_category_id
    /* Draft Prospect Properties */
    , prospects.prospect_first_name
    , prospects.prospect_last_name
    , prospects.prospect_full_name
    , prospects.prospect_birth_date
    , prospects.prospect_age_years
    , prospects.prospect_age_days
    , prospects.prospect_birth_city
    , prospects.prospect_birth_state_province
    , prospects.prospect_birth_country
    , prospects.prospect_height
    , prospects.prospect_weight
    , prospects.prospect_shoots_catches
    , prospects.prospect_position_name
    , prospects.prospect_position_abbreviation
    , prospects.prospect_draft_status -- wtf is this?
    , prospects.prospect_category_name
    , prospects.prospect_category_short_name
    , prospects.prospect_amateur_team_name
    , prospects.prospect_amateur_team_url
    , prospects.prospect_amateur_league_name
    , prospects.prospect_amateur_league_url
    , prospects.prospect_rank_midterm
    , prospects.prospect_rank_draft_year
    , prospects.prospect_url
from 
    {{ ref('stg_nhl__draft_prospects')}} as prospects

