select
    /* Primary Key */
    boxscore_team.id
    /* Foreign Keys */
    , boxscore_team.game_id
    , boxscore_team.team_id
    /* Properties */
    , boxscore_team.team_type
    /* Team stats*/
    , boxscore_team.team_name
    , boxscore_team.team_winner
    , boxscore_team.team_goals
    , boxscore_team.team_goal_differential
    , boxscore_team.team_pim
    , boxscore_team.team_shots
    , boxscore_team.team_powerplay_goals
    , boxscore_team.team_powerplay_opportunities
    , boxscore_team.team_faceoff_percentage
    , boxscore_team.team_blocked
    , boxscore_team.team_takeaways
    , boxscore_team.team_giveaways
    , boxscore_team.team_hits
from 
    {{ ref('stg_nhl__boxscore_team') }} as boxscore_team
order by
    boxscore_team.game_id desc