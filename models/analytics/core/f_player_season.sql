with
player_season as (
    select
        bp.player_id
        , player.full_name as player_full_name
        , season.season_id
        , season.regular_season_start_date
        , season.regular_season_end_date
        , season.number_of_games as full_season_games
        , count(distinct bp.game_id) as boxscore_games
        , sum(ifnull(bp.assists, 0)) as assists
        , sum(ifnull(bp.goals, 0)) as goals
        , sum(ifnull(bp.shots, 0)) as shots
        , sum(ifnull(bp.hits, 0)) as hits
        , sum(bp.faceoff_wins) as faceoff_wins
        , sum(bp.faceoff_taken) as faceoff_taken
        , sum(ifnull(bp.takeaways, 0)) as takeaways
        , sum(ifnull(bp.giveaways, 0)) as giveaways
        , sum(ifnull(bp.blocked, 0)) as blocked
        , sum(ifnull(bp.plus_minus, 0)) as plus_minus
        , sum(ifnull(bp.pim, 0)) as pim
        , sum(cast(split(bp.time_on_ice, ':')[offset(0)] as int)) as time_on_ice_mins
        , sum(cast(split(bp.time_on_ice, ':')[offset(1)] as int)) as time_on_ice_seconds
        , sum(ifnull(bp.powerplay_goals, 0)) as powerplay_goals
        , sum(ifnull(bp.powerplay_assists, 0)) as powerplay_assists
        , sum(ifnull(bp.short_handed_goals, 0)) as short_handed_goals
        , sum(ifnull(bp.short_handed_assists, 0)) as short_handed_assists
        , sum(ifnull(bp.saves, 0)) as saves
        , sum(ifnull(bp.powerplay_saves, 0)) as powerplay_saves
        , sum(ifnull(bp.even_saves, 0)) as even_saves
        , sum(ifnull(bp.short_handed_shots_against, 0)) as shorthanded_shots_against
        , sum(ifnull(bp.even_shots_against, 0)) as even_shots_against
        , sum(ifnull(bp.powerplay_shots_against, 0)) as powerplay_shots_against
    --,sum(case when bp.decision = "W" then 1 else 0) as wins
    --,sum(case when bp.decision = "L" then 1 else 0) as losses
    from {{ ref('f_boxscore_player') }} as bp
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = bp.game_id
    left join {{ ref('d_seasons') }} as season on season.season_id = schedule.season_id
    left join {{ ref('d_players') }} as player on player.player_id = bp.player_id
    left join {{ ref('f_games_scratches') }} as scratches on scratches.player_id = bp.player_id and scratches.game_id = bp.game_id

    where 1 = 1
        and schedule.game_type = '02' --regular season only
        and scratches.player_id is null -- remove scractches, should be same as `and bp.time_on_ice is not null`
    group by
        bp.player_id
        , player.full_name
        , season.season_id
        , season.number_of_games
        , season.regular_season_start_date
        , season.regular_season_end_date
    order by
        count(distinct bp.game_id) desc
        , sum(bp.goals) desc
)

-- at the player-season level, get the number of missed shots and blocked shots by each shooter
, player_stats as (
    select
        plays.player_id
        , season.season_id
        , max(plays.event_description) as example_eventdescription
        -- shot types
<<<<<<< HEAD
        , sum(case when plays.event_secondary_type = "BACKHAND" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_backhand_all
        , sum(case when plays.event_secondary_type = "DEFLECTED" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_deflected_all
        , sum(case when plays.event_secondary_type = "SLAP SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_slapshot_all
        , sum(case when plays.event_secondary_type = "SNAP SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_snapshot_all
        , sum(case when plays.event_secondary_type = "TIP-IN" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_tipin_all
        , sum(case when plays.event_secondary_type = "WRAP-AROUND" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wraparound_all
        , sum(case when plays.event_secondary_type = "WRIST SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wristshot_all
        -- shot results
=======
>>>>>>> f8d41389fe2cc8011112fd641c1fbab6e4537933
        , sum(case when plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_blocked
        , sum(case when plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_missed
        , sum(case when plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_saved
        , sum(case when plays.event_type = "GOAL" and plays.player_role = "SCORER" then 1 else 0 end) as shots_scored
<<<<<<< HEAD
        -- shot types by result
        /**----- shot-blocked
        , sum(case when plays.event_secondary_type = "BACKHAND" and plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_backhand_blocked
        , sum(case when plays.event_secondary_type = "DEFLECTED" and plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER"then 1 else 0 end) as shots_deflected_blocked
        , sum(case when plays.event_secondary_type = "SLAP SHOT" and plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_slapshot_blocked
        , sum(case when plays.event_secondary_type = "SNAP SHOT" and plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_snapshot_blocked
        , sum(case when plays.event_secondary_type = "TIP-IN" and plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_tipin_blocked
        , sum(case when plays.event_secondary_type = "WRAP-AROUND" and plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wraparound_blocked
        , sum(case when plays.event_secondary_type = "WRIST SHOT" and plays.event_type = "BLOCKED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wristshot_blocked
        ----- shot-missed
        , sum(case when plays.event_secondary_type = "BACKHAND" and plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_backhand_missed
        , sum(case when plays.event_secondary_type = "DEFLECTED" and plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER"then 1 else 0 end) as shots_deflected_missed
        , sum(case when plays.event_secondary_type = "SLAP SHOT" and plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_slapshot_missed
        , sum(case when plays.event_secondary_type = "SNAP SHOT" and plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_snapshot_missed
        , sum(case when plays.event_secondary_type = "TIP-IN" and plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_tipin_missed
        , sum(case when plays.event_secondary_type = "WRAP-AROUND" and plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wraparound_missed
        , sum(case when plays.event_secondary_type = "WRIST SHOT" and plays.event_type = "MISSED_SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wristshot_missed
        **/
        ----- shot-saved
        , sum(case when plays.event_secondary_type = "BACKHAND" and plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_backhand_saved
        , sum(case when plays.event_secondary_type = "DEFLECTED" and plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_deflected_saved
        , sum(case when plays.event_secondary_type = "SLAP SHOT" and plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_slapshot_saved
        , sum(case when plays.event_secondary_type = "SNAP SHOT" and plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_snapshot_saved
        , sum(case when plays.event_secondary_type = "TIP-IN" and plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_tipin_saved
        , sum(case when plays.event_secondary_type = "WRAP-AROUND" and plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wraparound_saved
        , sum(case when plays.event_secondary_type = "WRIST SHOT" and plays.event_type = "SHOT" and plays.player_role = "SHOOTER" then 1 else 0 end) as shots_wristshot_saved
        ----- shot-goal
        , sum(case when plays.event_secondary_type = "BACKHAND" and plays.player_role = "SCORER" then 1 else 0 end) as shots_backhand_goal
        , sum(case when plays.event_secondary_type = "DEFLECTED" and plays.player_role = "SCORER" then 1 else 0 end) as shots_deflected_goal
        , sum(case when plays.event_secondary_type = "SLAP SHOT" and plays.player_role = "SCORER" then 1 else 0 end) as shots_slapshot_goal
        , sum(case when plays.event_secondary_type = "SNAP SHOT" and plays.player_role = "SCORER" then 1 else 0 end) as shots_snapshot_goal
        , sum(case when plays.event_secondary_type = "TIP-IN" and plays.player_role = "SCORER" then 1 else 0 end) as shots_tipin_goal
        , sum(case when plays.event_secondary_type = "WRAP-AROUND" and plays.player_role = "SCORER" then 1 else 0 end) as shots_wraparound_goal
        , sum(case when plays.event_secondary_type = "WRIST SHOT" and plays.player_role = "SCORER" then 1 else 0 end) as shots_wristshot_goal
=======
>>>>>>> f8d41389fe2cc8011112fd641c1fbab6e4537933
        -- goal types
        , sum(case when plays.last_goal_game_winning = 1 and plays.player_role = "SCORER" then 1 else 0 end) as goals_gamewinning
        , sum(case when (plays.home_result_of_play = 'Chase goal' or plays.away_result_of_play = 'Chase goal') and plays.player_role = "SCORER" then 1 else 0 end) as goals_chasegoal
        , sum(case when (plays.home_result_of_play = 'Tying goal scored' or plays.away_result_of_play = 'Tying goal scored') and plays.player_role = "SCORER" then 1 else 0 end) as goals_gametying
        , sum(case when (plays.home_result_of_play = 'Go-ahead goal scored' or plays.away_result_of_play = 'Go-ahead goal scored') and plays.player_role = "SCORER" then 1 else 0 end) as goals_goahead
        , sum(case when (plays.home_result_of_play = 'Buffer goal' or plays.away_result_of_play = 'Buffer goal') and plays.player_role = "SCORER" then 1 else 0 end) as goals_buffergoal
        -- assist types
        , sum(case when plays.player_primary_assist = true and plays.player_role = "ASSIST" and plays.event_type = "GOAL" then 1 else 0 end) as assists_primary
        , sum(case when plays.player_secondary_assist = true and plays.player_role = "ASSIST" and plays.event_type = "GOAL" then 1 else 0 end) as assists_secondary
    from {{ ref('f_plays') }} as plays
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = plays.game_id
    left join {{ ref('d_seasons') }} as season on season.season_id = schedule.season_id
    where 1 = 1
        and plays.player_role in ("SHOOTER", "SCORER", "ASSIST")
        and plays.event_type in ("BLOCKED_SHOT", "MISSED_SHOT", "SHOT", "GOAL")
        and plays.play_period_type <> 'SHOOTOUT'
<<<<<<< HEAD
        and schedule.game_type = '02'
=======
        and schedule.game_type = 'R'
>>>>>>> f8d41389fe2cc8011112fd641c1fbab6e4537933
    group by
        plays.player_id
        , season.season_id
    order by
        sum(case when plays.event_type = "GOAL" and plays.player_role = "SCORER" then 1 else 0 end) desc
)

select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['player_season.player_id', 'player_season.season_id']) }} as player_season_id
    /* Foreign Keys */
    , player_season.player_id
    , player_season.season_id
    /* Season Properties */
    , player_season.regular_season_start_date
    , player_season.regular_season_end_date
    , player_season.full_season_games
    , player_season.boxscore_games
    /* Player Properties */
    , player_season.player_full_name
    /* Player Stats */
    -- Time on Ice
    , (player_season.time_on_ice_mins * 60) + (player_season.time_on_ice_seconds) as time_on_ice_seconds
    , round((player_season.time_on_ice_mins) + (player_season.time_on_ice_seconds / 60), 2) as time_on_ice_minutes
    , round(((player_season.time_on_ice_mins) + (player_season.time_on_ice_seconds / 60)) / player_season.boxscore_games, 2) as avg_time_on_ice_mins
    -- Goal-scoring skater events (Goals, Assists, Points)
    , player_season.goals
    , (player_season.goals / player_season.boxscore_games) as goals_pergame
    , player_stats.goals_gamewinning
    , player_stats.goals_chasegoal
    , player_stats.goals_gametying
    , player_stats.goals_goahead
    , player_stats.goals_buffergoal
    , player_season.assists
    , player_stats.assists_primary
    , player_stats.assists_secondary
    , player_season.goals + player_season.assists as points
    , ((player_season.goals + player_season.assists) / player_season.boxscore_games) as points_pergame
    -- Shot results
    , player_stats.shots_blocked + player_stats.shots_missed + player_stats.shots_saved + player_stats.shots_scored as shots_all
    , player_season.shots as shots_ongoal
    , player_stats.shots_blocked
    , player_stats.shots_missed
    , player_stats.shots_saved
    , player_stats.shots_scored
    -- Shot types
    , player_stats.shots_backhand_all
    , player_stats.shots_deflected_all
    , player_stats.shots_slapshot_all
    , player_stats.shots_snapshot_all
    , player_stats.shots_tipin_all
    , player_stats.shots_wraparound_all
    , player_stats.shots_wristshot_all
    -- Shot results by type
    ----- shot-saved
    , player_stats.shots_backhand_saved
    , player_stats.shots_deflected_saved
    , player_stats.shots_slapshot_saved
    , player_stats.shots_snapshot_saved
    , player_stats.shots_tipin_saved
    , player_stats.shots_wraparound_saved
    , player_stats.shots_wristshot_saved
    ----- shot-goal
    , player_stats.shots_backhand_goal
    , player_stats.shots_deflected_goal
    , player_stats.shots_slapshot_goal
    , player_stats.shots_snapshot_goal
    , player_stats.shots_tipin_goal
    , player_stats.shots_wraparound_goal
    , player_stats.shots_wristshot_goal
    ---- shot-conversion #TODO filter to a specific number of shots to be in consideration
    , case when player_season.shots < 1 then 0 else round((player_season.goals / player_season.shots), 2) end as pcnt_shooting_all
    , case when (player_stats.shots_blocked + player_stats.shots_missed + player_stats.shots_saved + player_stats.shots_scored) < 1 then 0 else round((player_season.goals / (player_stats.shots_blocked + player_stats.shots_missed + player_stats.shots_saved + player_stats.shots_scored)), 2) end as pcnt_shooting_ongoal
    , case when player_stats.shots_backhand_all < 1 then 0 else round((player_stats.shots_backhand_goal / player_stats.shots_backhand_all), 2) end as pcnt_shooting_backhand
    , case when player_stats.shots_deflected_all < 1 then 0 else round((player_stats.shots_deflected_goal / player_stats.shots_deflected_all), 2) end as pcnt_shooting_deflected
    , case when player_stats.shots_slapshot_all < 1 then 0 else round((player_stats.shots_slapshot_goal / player_stats.shots_slapshot_all), 2) end as pcnt_shooting_slapshot
    , case when player_stats.shots_snapshot_all < 1 then 0 else round((player_stats.shots_snapshot_goal / player_stats.shots_snapshot_all), 2) end as pcnt_shooting_snapshot
    , case when player_stats.shots_tipin_all < 1 then 0 else round((player_stats.shots_tipin_goal / player_stats.shots_tipin_all), 2) end as pcnt_tipin_sog
    , case when player_stats.shots_wraparound_all < 1 then 0 else round((player_stats.shots_wraparound_goal / player_stats.shots_wraparound_all), 2) end as pcnt_shooting_wraparound
    , case when player_stats.shots_wristshot_all < 1 then 0 else round((player_stats.shots_wristshot_goal / player_stats.shots_wristshot_all), 2) end as pcnt_shooting_wristshot
    -- Other skater events
    , player_season.faceoff_wins
    , player_season.faceoff_taken
    , case when player_season.faceoff_taken < 1 then 0 else round((player_season.faceoff_wins / player_season.faceoff_taken), 2) end as pcnt_faceoffwins
    , player_season.hits
    , player_season.takeaways
    , player_season.giveaways
    , player_season.blocked
    , player_season.plus_minus
    , player_season.pim
    -- Special teams skater events
    , player_season.powerplay_goals
    , player_season.powerplay_assists
    , player_season.short_handed_goals
    , player_season.short_handed_assists
    -- Goalie events
    , player_season.saves
    , player_season.powerplay_saves
    , player_season.even_saves
    , player_season.shorthanded_shots_against
    , player_season.even_shots_against
    , player_season.powerplay_shots_against
--,player_season.wins
--,player_season.losses
from player_season
left join player_stats on player_season.player_id = player_stats.player_id and player_season.season_id = player_stats.season_id
order by
    player_season.goals desc