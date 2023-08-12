with

boxscore_reduced as (
  /* stg_nhl__boxscore is on the game/team/player grain, so reduce down to the game/team grain */
  select * from {{ ref('stg_nhl__boxscore') }}
  qualify row_number() over (
    partition by
      game_id
      , team_id
  ) = 1
)

, home_team as (
  select * from boxscore_reduced where team_type = 'Home'
)

, away_team as (
  select * from boxscore_reduced where team_type = 'Away'
)

, winning_team as (
  select
    home_team.game_id
    , home_team.team_id as home_team_id
    , away_team.team_id as away_team_id
    , home_team.team_goals as home_team_score
    , away_team.team_goals as away_team_score
    , case
      when home_team.team_goals > away_team.team_goals then home_team.team_type
      when home_team.team_goals < away_team.team_goals then away_team.team_type
      when home_team.team_goals = away_team.team_goals then 'Tie'
      else 'Undetermined'
    end as winning_team
    , coalesce(abs(home_team.team_goals - away_team.team_goals), 0) as absolute_goal_differential
  from home_team
  inner join away_team on home_team.game_id = away_team.game_id
)

, boxscore_team as (
  select * from home_team
  union all
  select * from away_team
)

select
  /* Primary Key */
  {{ dbt_utils.surrogate_key(['boxscore_team.game_id', 'boxscore_team.team_id']) }} as boxscore_team_id

  /* Foreign Keys */
  , boxscore_team.game_id
  , boxscore_team.team_id

  /* Properties */
  , boxscore_team.team_type

  /* Team stats*/
  , boxscore_team.team_name
  , boxscore_team.team_type = winning_team.winning_team as team_winner
  , boxscore_team.team_goals
  , if(
    boxscore_team.team_type = winning_team.winning_team
    , winning_team.absolute_goal_differential
    , -1 * winning_team.absolute_goal_differential
  ) as team_goal_differential
  , boxscore_team.team_pim
  , boxscore_team.team_shots
  , boxscore_team.team_powerplay_goals
  , boxscore_team.team_powerplay_opportunities
  , boxscore_team.team_faceoff_percentage
  , boxscore_team.team_blocked
  , boxscore_team.team_takeaways
  , boxscore_team.team_giveaways
  , boxscore_team.team_hits
  , boxscore_team.team_scratches
from boxscore_team
left join winning_team on boxscore_team.game_id = winning_team.game_id
