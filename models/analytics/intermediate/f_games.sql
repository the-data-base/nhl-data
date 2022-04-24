select
    /* Primary Key */
    games.id
    /* Foreign Keys */
    , games.home_team_id
    , games.away_team_id
    /* Properties */
    -- Game-level stats
    , games.game_score_description
    , games.game_matchup_description
    , games.game_winning_team_id
    , games.game_winning_team_name
    , games.game_absolute_goal_differential
    , games.home_team_goals
    , games.away_team_goals
    -- Home team stats
    , games.home_team_name
    , games.home_team_pim
    , games.home_team_shots
    , games.home_team_powerplay_goals
    , games.home_team_powerplay_opportunities
    , games.home_team_faceoff_percentage
    , games.home_team_blocked
    , games.home_team_takeaways
    , games.home_team_giveaways
    , games.home_team_hits
    -- Away team stats
    , games.away_team_name
    , games.away_team_pim
    , games.away_team_shots
    , games.away_team_powerplay_goals
    , games.away_team_powerplay_opportunities
    , games.away_team_faceoff_percentage
    , games.away_team_blocked
    , games.away_team_takeaways
    , games.away_team_giveaways
    , games.away_team_hits
from 
    {{ ref('stg_nhl__games') }} AS games
