with t1 as (
  select
    boxscore.*
    , schedule.season_id
    , schedule.game_type_description
    , schedule.game_type
  from {{ ref('f_boxscore_team') }} as boxscore
  left join {{ ref('d_schedule') }} as schedule
    on boxscore.game_id = schedule.game_id
)

, team_season as (
  select
    team_id
    , team_name
    , season_id
    , game_type
    , game_type_description
    , count(*) as games_played
    , sum(case when team_winner = true then 1 else 0 end) as wins
    , sum(ifnull(team_goals, 0)) as team_goals
    , sum(ifnull(team_goal_differential, 0)) as team_goal_differential
    , sum(ifnull(team_pim, 0)) as team_pim
    , sum(ifnull(team_shots, 0)) as team_shots
    , round(sum(ifnull(team_powerplay_goals, 0)), 0) as team_powerplay_goals
    , sum(ifnull(team_hits, 0)) as team_hits
    , sum(ifnull(team_blocked, 0)) as team_blocked
    , sum(ifnull(team_takeaways, 0)) as team_takeaways
    , sum(ifnull(team_giveaways, 0)) as team_giveaways
  from t1
  group by 1, 2, 3, 4, 5
)

select
  /* Primary Key */
  {{ dbt_utils.surrogate_key(['team_season.team_id', 'team_season.season_id', 'team_season.game_type_description']) }} as team_season_stage_id
  /* Foreign Keys */
  , team_season.team_id
  , team_season.season_id
  /* Team & Season Properties */
  , team_season.team_name
  , team_season.game_type
  , team_season.game_type_description
  , games_played
  /* Team Stats */
  , wins
  , team_goals
  , team_goal_differential
  , team_pim
  , team_shots
  , team_powerplay_goals
  , team_hits
  , team_blocked
  , team_giveaways
  , team_takeaways
from team_season
order by team_season.season_id, team_season.game_type
