with

home_team_scratches as (
  select
    boxscore.game_id
    , scratches as player_id
  from {{ ref('f_boxscore_team') }} as boxscore
  , unnest(boxscore.team_scratches) as scratches
  where team_type = 'Home'
)

, away_team_scratches as (
  select
    boxscore.game_id
    , scratches as player_id
  from {{ ref('f_boxscore_team') }} as boxscore
  , unnest(boxscore.team_scratches) as scratches
  where team_type = 'Away'
)

, unioned as (
  select
    game_id
    , player_id
  from home_team_scratches

  union all

  select
    game_id
    , player_id
  from away_team_scratches
)

select * from unioned
