select
    /* Primary Key */
    id

    /* Foreign Keys */
    , game_id
    , home_team_id
    , away_team_id
    , game_winning_team_id

    /* Properties */
    -- Game-level stats
    , game_score_description
    , game_matchup_description
    , game_winning_team_name
    , game_winning_team_type
    , game_absolute_goal_differential
    -- Team level stats
    , home_team_goals
    , away_team_goals
from {{ ref('stg_nhl__linescore') }}
