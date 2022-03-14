select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['stg_meltano__boxscore.id', 'stg_meltano__boxscore.player_id']) }} as id

    /* Foreign Keys */
    , stg_meltano__boxscore.game_id
    , stg_meltano__boxscore.player_id
    , f_boxscore_game.away_team_id
    , f_boxscore_game.home_team_id

    /* Properties */
    , stg_meltano__boxscore.is_home_player
    -- Team level
    , f_boxscore_game.away_team_goals
    , f_boxscore_game.away_team_pim
    , f_boxscore_game.away_team_shots
    , f_boxscore_game.away_team_powerplay_goals
    , f_boxscore_game.away_team_powerplay_opportunities
    , f_boxscore_game.away_team_faceoff_percentage
    , f_boxscore_game.away_team_blocked
    , f_boxscore_game.away_team_takeaways
    , f_boxscore_game.away_team_giveaways
    , f_boxscore_game.away_team_hits
    , f_boxscore_game.home_team_goals
    , f_boxscore_game.home_team_pim
    , f_boxscore_game.home_team_shots
    , f_boxscore_game.home_team_powerplay_goals
    , f_boxscore_game.home_team_powerplay_opportunities
    , f_boxscore_game.home_team_faceoff_percentage
    , f_boxscore_game.home_team_blocked
    , f_boxscore_game.home_team_takeaways
    , f_boxscore_game.home_team_giveaways
    , f_boxscore_game.home_team_hits

    -- Player level
    , stg_meltano__boxscore.roster_status as player_roster_status
    , stg_meltano__boxscore.position_code as player_position_code
    , stg_meltano__boxscore.time_on_ice as player_toi
    , stg_meltano__boxscore.goals as player_goals
    , stg_meltano__boxscore.assists as player_assits
    , stg_meltano__boxscore.shots as player_shots
    , stg_meltano__boxscore.hits as player_hits
    , stg_meltano__boxscore.powerplay_goals as player_powerplay_goals
    , stg_meltano__boxscore.powerplay_assists as player_powerplay_asissts
    , stg_meltano__boxscore.penalty_minutes as player_penalty_minutes
    , stg_meltano__boxscore.faceoff_wins as player_faceoff_wins
    , stg_meltano__boxscore.faceoff_taken as player_faceoff_taken
    , stg_meltano__boxscore.takeaways as player_takeaways
    , stg_meltano__boxscore.giveaways as player_giveaways
    , stg_meltano__boxscore.short_handed_goals as player_shorthanded_goals
    , stg_meltano__boxscore.short_handed_assists as player_shorthanded_assists
    , stg_meltano__boxscore.blocked as player_blocked
    , stg_meltano__boxscore.plus_minus as player_plus_minus
    , stg_meltano__boxscore.even_time_on_ice as player_even_toi
    , stg_meltano__boxscore.powerplay_time_on_ice as player_powerplay_toi
    , stg_meltano__boxscore.short_handed_time_on_ice as player_shorthanded_toi
    , stg_meltano__boxscore.pim as player_pim
    , stg_meltano__boxscore.saves as player_saves
    , stg_meltano__boxscore.powerplay_saves as player_powerplay_saves
    , stg_meltano__boxscore.short_handed_saves as player_shorthanded_saves
    , stg_meltano__boxscore.even_saves as player_even_saves
    , stg_meltano__boxscore.short_handed_shots_against as player_shorthanded_shots_against
    , stg_meltano__boxscore.even_shots_against as player_even_shots_against
    , stg_meltano__boxscore.powerplay_shots_against as player_powerplay_shots_against
    , stg_meltano__boxscore.decision as player_decision
    , stg_meltano__boxscore.save_percentage as player_save_percentage
    , stg_meltano__boxscore.powerplay_save_percentage as player_powerplay_save_percentage
    , stg_meltano__boxscore.even_strength_save_percentage as player_even_save_percentage

from
    {{ ref('stg_meltano__boxscore_player') }}
    left join {{ ref('f_boxscore_game') }} on stg_meltano__boxscore.game_id = f_boxscore_game.game_id

order by
    stg_meltano__boxscore.id desc
