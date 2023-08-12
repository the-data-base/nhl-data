with

home_team as (
  select * from {{ ref('stg_nhl__boxscore') }} where team_type = 'Home'
)

, away_team as (
  select * from {{ ref('stg_nhl__boxscore') }} where team_type = 'Away'
)

, boxscore_player as (
  select * from home_team
  union all
  select * from away_team
)

select
  /* Primary Key */
  stg_nhl__boxscore_id as boxscore_player_id

  /* Foreign Keys */
  , game_id
  , team_id
  , player_id

  /* Properties */
  , team_name
  , team_type

  /* Player stats */
  , player_full_name
  , player_roster_status
  , player_position_code
  , time_on_ice
  , assists
  , goals
  , shots
  , hits
  , powerplay_goals
  , powerplay_assists
  , penalty_minutes
  , faceoff_wins
  , faceoff_taken
  , takeaways
  , giveaways
  , short_handed_goals
  , short_handed_assists
  , blocked
  , plus_minus
  , even_time_on_ice
  , powerplay_time_on_ice
  , short_handed_time_on_ice
  , pim
  , saves
  , powerplay_saves
  , short_handed_saves
  , even_saves
  , short_handed_shots_against
  , even_shots_against
  , powerplay_shots_against
  , decision
  , save_percentage
  , powerplay_save_percentage
  , even_strength_save_percentage
from boxscore_player
