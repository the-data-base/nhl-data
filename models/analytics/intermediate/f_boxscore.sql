select
    /* Primary Key */
    boxscore.id
    /* Foreign Keys */
    , boxscore.game_id
    , boxscore.home_team_id
    , boxscore.away_team_id
    /* Properties */
    -- Home team stats
    , boxscore.home_team_name
    , boxscore.home_team_pim
    , boxscore.home_team_shots
    , boxscore.home_team_powerplay_goals
    , boxscore.home_team_powerplay_opportunities
    , boxscore.home_team_faceoff_percentage
    , boxscore.home_team_blocked
    , boxscore.home_team_takeaways
    , boxscore.home_team_giveaways
    , boxscore.home_team_hits
    -- Away team stats
    , boxscore.away_team_name
    , boxscore.away_team_pim
    , boxscore.away_team_shots
    , boxscore.away_team_powerplay_goals
    , boxscore.away_team_powerplay_opportunities
    , boxscore.away_team_faceoff_percentage
    , boxscore.away_team_blocked
    , boxscore.away_team_takeaways
    , boxscore.away_team_giveaways
    , boxscore.away_team_hits
from
    {{ ref('stg_nhl__boxscore') }} as boxscore
