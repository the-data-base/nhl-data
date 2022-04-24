select
    /* Primary Key */
    boxscore_player.id
    /* Foreign Keys */
    , boxscore_player.game_id
    , boxscore_player.team_id
    , boxscore_player.player_id
    /* Properties */
    , boxscore_player.team_name
    , boxscore_player.team_type
    /* Player stats */
    , boxscore_player.player_full_name
    , boxscore_player.player_roster_status
    , boxscore_player.player_position_code
    , boxscore_player.time_on_ice
    , boxscore_player.assists
    , boxscore_player.goals
    , boxscore_player.shots
    , boxscore_player.hits
    , boxscore_player.powerplay_goals
    , boxscore_player.powerplay_assists
    , boxscore_player.penalty_minutes
    , boxscore_player.faceoff_wins
    , boxscore_player.faceoff_taken
    , boxscore_player.takeaways
    , boxscore_player.giveaways
    , boxscore_player.short_handed_goals
    , boxscore_player.short_handed_assists
    , boxscore_player.blocked
    , boxscore_player.plus_minus
    , boxscore_player.even_time_on_ice
    , boxscore_player.powerplay_time_on_ice
    , boxscore_player.short_handed_time_on_ice
    , boxscore_player.pim
    , boxscore_player.saves
    , boxscore_player.powerplay_saves
    , boxscore_player.short_handed_saves
    , boxscore_player.even_saves
    , boxscore_player.short_handed_shots_against
    , boxscore_player.even_shots_against
    , boxscore_player.powerplay_shots_against
    , boxscore_player.decision
    , boxscore_player.save_percentage
    , boxscore_player.powerplay_save_percentage
    , boxscore_player.even_strength_save_percentage
from
    {{ ref('stg_nhl__boxscore_player') }} as boxscore_player
order by
    boxscore_player.game_id desc
    , boxscore_player.team_id desc
    , boxscore_player.player_id desc
