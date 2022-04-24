select
    /* Primary Key */
    linescore.id
    /* Foreign Keys */
    , linescore.game_id
    , linescore.home_team_id
    , linescore.away_team_id
    , linescore.game_winning_team_id
    /* Properties */
    -- Game-level stats
    , linescore.game_score_description
    , linescore.game_matchup_description
    , linescore.game_winning_team_name
    , linescore.game_winning_team_type
    , linescore.game_absolute_goal_differential
    -- Team level stats
    , linescore.home_team_goals
    , linescore.away_team_goals
from 
    {{ ref('stg_nhl__linescore') }} as linescore
