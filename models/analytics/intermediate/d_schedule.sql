select
    /* Primary Key */
    schedule.game_id
    /* Foreign Keys */
    , schedule.season_id
    , schedule.away_team_id
    , schedule.home_team_id
    , schedule.venue_id
    /* Schedule Properties */
    , schedule.game_number
    , schedule.url
    , schedule.game_type
    , schedule.game_date
    , schedule.abstract_game_state
    , schedule.coded_game_state
    , schedule.detailed_state
    , schedule.status_code
    , schedule.is_start_time_tbd
    , schedule.away_team_wins
    , schedule.away_team_losses
    , schedule.away_team_ot
    , schedule.away_team_type
    , schedule.away_team_score
    , schedule.away_team_name
    , schedule.away_team_url
    , schedule.home_team_wins
    , schedule.home_team_losses
    , schedule.home_team_ot
    , schedule.home_team_type
    , schedule.home_team_score
    , schedule.home_team_name
    , schedule.home_team_url
    , schedule.venue_name
    , schedule.venue_url
    , schedule.content_url
    , schedule.extracted_at
    , schedule.loaded_at
from 
    {{ ref('stg_nhl__schedule') }} as schedule
