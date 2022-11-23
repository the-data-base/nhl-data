with
--cte#1: basis for player-season-game_type level data
boxscore_stats as (
    select
        bp.player_id
        , player.full_name as player_full_name
        , schedule.game_type
        , schedule.game_type_description
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
    --,sum(case when bp.decision = "w" then 1 else 0) as wins
    --,sum(case when bp.decision = "l" then 1 else 0) as losses
    from {{ ref('f_boxscore_player') }} as bp
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = bp.game_id
    left join {{ ref('d_seasons') }} as season on season.season_id = schedule.season_id
    left join {{ ref('d_players') }} as player on player.player_id = bp.player_id
    left join {{ ref('f_games_scratches') }} as scratches on scratches.player_id = bp.player_id and scratches.game_id = bp.game_id

    where 1 = 1
        -- keep only regular season and playoff games
        and schedule.game_type in ('02', '03')
        -- remove scratches (fyi... should be the same same as `and bp.time_on_ice is not null`)
        and scratches.player_id is null
    group by
        bp.player_id
        , player.full_name
        , season.season_id
        , season.number_of_games
        , season.regular_season_start_date
        , season.regular_season_end_date
        , schedule.game_type
        , schedule.game_type_description
    order by
        count(distinct bp.game_id) desc
        , sum(bp.goals) desc
)

-- #cte2: basis for on-ice-stats - play-by-play data for all shots, goals and assists
, shots_goals_assists as (
    select
        plays.play_id
        , plays.game_id
        , plays.player_id
        , season.season_id
        , team.team_id
        , team.team_name
        , schedule.game_type
        , last_goal_game_winning
        , player_primary_assist
        , player_secondary_assist
        , lower(schedule.game_type_description) as game_type_description
        , lower(plays.event_description) as event_description
        , lower(plays.event_type) as event_type
        , lower(plays.event_secondary_type) as event_secondary_type
        , lower(plays.player_role_team) as player_role_team
        , lower(plays.player_role) as player_role
        , lower(plays.play_period_type) as play_period_type
        , lower(home_result_of_play) as home_result_of_play
        , lower(away_result_of_play) as away_result_of_play
        , home_skaters
        , away_skaters
        , seconds_since_last_shot
        , shot_rebound_ind
        , last_shot_seconds
        , last_shot_rebound_ind
    from {{ ref('f_plays') }} as plays
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = plays.game_id
    left join {{ ref('d_seasons') }} as season on season.season_id = schedule.season_id
    left join {{ ref('d_teams') }} as team on team.team_id = plays.team_id
    where 1 = 1
        -- remove shootout plays
        and lower(plays.play_period_type) <> 'shootout'
        -- keep only regular season and playoff games
        and schedule.game_type in ('02', '03')
        -- keep roles involving the shooter, scorer or assister
        and lower(plays.player_role) in ('shooter', 'scorer', 'assist')
        -- keep blocked shots, missed shots, shots on target and goals
        and lower(plays.event_type) in ('blocked_shot', 'missed_shot', 'shot', 'goal')
)

-- cte#3: flatten all shot events on both arrays (home_skaters & away_skaters) so that we can get corsi & fenwick features
, shots_flat as (
    select
        *
        , player_role_team as shooting_team
        , player_id as shooter_player_id
        , 'home' as skater_type
    from shots_goals_assists
    , unnest(home_skaters) as shot_player_id
    where player_role in ('shooter', 'scorer')
    union all
    select
        *
        , player_role_team as shooting_team
        , player_id as shooter_player_id
        , 'away' as skater_type
    from shots_goals_assists
    , unnest(away_skaters) as shot_player_id
    where player_role in ('shooter', 'scorer')
)

-- cte#4: on-ice shots stats: shot & goal summaries
, shots_involvement as (
    select
        player.full_name as player_full_name
        , shot_player_id
        , game_id
        , game_type
        , season_id
        -- shot descriptors
        , case when shooting_team = skater_type then 'shot for' else 'shot against' end as shot_type
        , case
            when shooter_player_id = shot_player_id then 'shooter'
            when shooting_team = skater_type then 'shooter teammate'
            when shooting_team <> skater_type then 'shooter opponent'
        end as shooter_description
        , shooting_team
        , shooter_player_id
        , skater_type
        , event_type
        , event_secondary_type
        , shot_rebound_ind as shots_rebound
        , last_shot_rebound_ind as last_shot_rebound
        -- shot calculations
        , case when s.event_type in ('goal', 'shot') then 1 else 0 end as shots_ongoal
        , case when s.event_type = 'blocked_shot' then 1 else 0 end as shots_blocked
        , case when s.event_type = 'missed_shot' then 1 else 0 end as shots_missed
        , case when s.event_type = 'shot' then 1 else 0 end as shots_saved
        , case when s.event_type = 'goal' then 1 else 0 end as shots_scored
        , case when s.event_type in ('blocked_shot', 'missed_shot', 'shot', 'goal') then 1 else 0 end as corsi_shot
        , case when s.event_type in ('missed_shot', 'shot', 'goal') then 1 else 0 end as fenwick_shot
    from shots_flat as s
    left join {{ ref('d_players') }} as player on player.player_id = s.shot_player_id
    where lower(primary_position_code) <> 'g'
)

-- cte#5: on-ice shots stats: summarizing individual shots (i) and on-ice shots
, onice_shots_stats as (
    select
        game_type
        , season_id
        , shot_player_id
        , player_full_name
        -- on-ice shot calculations: fenwick shots for (ff), & fenwick shots against (fa)
        , sum(case when shot_type = 'shot for' then fenwick_shot else 0 end) as shots_ff
        , sum(case when shot_type = 'shot against' then fenwick_shot else 0 end) as shots_fa
        -- on-ice shot calculations: corsi shots for (cf), & corsi shots against (ca)
        , sum(case when shot_type = 'shot for' then corsi_shot else 0 end) as shots_cf
        , sum(case when shot_type = 'shot against' then corsi_shot else 0 end) as shots_ca
        -- on-ice shot calculations: shots for (sf), and shots against (sa)
        , sum(case when shot_type = 'shot for' then shots_ongoal else 0 end) as shots_sf
        , sum(case when shot_type = 'shot against' then shots_ongoal else 0 end) as shots_sa
        -- on-ice shot calculations: goals for (gf), and goals against (ga)
        , sum(case when shot_type = 'shot for' then shots_scored else 0 end) as shots_gf
        , sum(case when shot_type = 'shot against' then shots_scored else 0 end) as shots_ga
        -- individual (i) shot calculations: shots-on-goal, fenwick-for, & corsi-for
        , sum(case when shooter_description = 'shooter' then shots_ongoal else 0 end) as shots_isog
        , sum(case when shooter_description = 'shooter' then fenwick_shot else 0 end) as shots_iff
        , sum(case when shooter_description = 'shooter' then corsi_shot else 0 end) as shots_icf
        , sum(case when shooter_description = 'shooter' then shots_blocked else 0 end) as shots_iblocked
        , sum(case when shooter_description = 'shooter' then shots_missed else 0 end) as shots_imissed
        , sum(case when shooter_description = 'shooter' then shots_saved else 0 end) as shots_isaved
        , sum(case when shooter_description = 'shooter' then shots_scored else 0 end) as shots_iscored
        -- individual shot calculations: (for shots on net) broken down by shot type, & shot results
        ----- all individual shot types (corsi)
        , sum(case when event_secondary_type = 'backhand' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_backhand_all
        , sum(case when event_secondary_type = 'deflected' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_deflected_all
        , sum(case when event_secondary_type = 'slap shot' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_slapshot_all
        , sum(case when event_secondary_type = 'snap shot' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_snapshot_all
        , sum(case when event_secondary_type = 'tip-in' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_tipin_all
        , sum(case when event_secondary_type = 'wrap-around' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_wraparound_all
        , sum(case when event_secondary_type = 'wrist shot' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_wristshot_all
        , sum(case when shots_rebound = 1 and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_rebound_all
        ----- shot-saved (shots on-goal that were saved)
        , sum(case when event_secondary_type = 'backhand' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_backhand_saved
        , sum(case when event_secondary_type = 'deflected' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_deflected_saved
        , sum(case when event_secondary_type = 'slap shot' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_slapshot_saved
        , sum(case when event_secondary_type = 'snap shot' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_snapshot_saved
        , sum(case when event_secondary_type = 'tip-in' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_tipin_saved
        , sum(case when event_secondary_type = 'wrap-around' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_wraparound_saved
        , sum(case when event_secondary_type = 'wrist shot' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_wristshot_saved
        , sum(case when shots_rebound = 1 and shooter_description = 'shooter' then shots_saved else 0 end) as shots_rebound_saved
        ----- shot-goal (shots on-goal that were goals)
        , sum(case when event_secondary_type = 'backhand' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_backhand_goal
        , sum(case when event_secondary_type = 'deflected' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_deflected_goal
        , sum(case when event_secondary_type = 'slap shot' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_slapshot_goal
        , sum(case when event_secondary_type = 'snap shot' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_snapshot_goal
        , sum(case when event_secondary_type = 'tip-in' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_tipin_goal
        , sum(case when event_secondary_type = 'wrap-around' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_wraparound_goal
        , sum(case when event_secondary_type = 'wrist shot' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_wristshot_goal
        , sum(case when shots_rebound = 1 and shooter_description = 'shooter' then shots_scored else 0 end) as shots_rebound_goal
    from shots_involvement
    group by 1, 2, 3, 4
)

-- cte#6: on-ice goal stats: goal & assist summaries
, onice_goals_stats as (
    select
        sga.player_id
        , sga.season_id
        , sga.game_type
        , max(sga.event_description) as example_eventdescription
        -- goal types
        , sum(case when sga.last_goal_game_winning = 1 and sga.player_role = 'scorer' then 1 else 0 end) as goals_gamewinning
        , sum(case when (sga.home_result_of_play = 'chase goal' or sga.away_result_of_play = 'chase goal') and sga.player_role = 'scorer' then 1 else 0 end) as goals_chasegoal
        , sum(case when (sga.home_result_of_play = 'tying goal scored' or sga.away_result_of_play = 'tying goal scored') and sga.player_role = 'scorer' then 1 else 0 end) as goals_gametying
        , sum(case when (sga.home_result_of_play = 'go-ahead goal scored' or sga.away_result_of_play = 'go-ahead goal scored') and sga.player_role = 'scorer' then 1 else 0 end) as goals_goahead
        , sum(case when (sga.home_result_of_play = 'buffer goal' or sga.away_result_of_play = 'buffer goal') and sga.player_role = 'scorer' then 1 else 0 end) as goals_buffergoal
        -- assist types
        , sum(case when sga.player_primary_assist = true and sga.player_role = 'assist' and sga.event_type = 'goal' then 1 else 0 end) as assists_primary
        , sum(case when sga.player_secondary_assist = true and sga.player_role = 'assist' and sga.event_type = 'goal' then 1 else 0 end) as assists_secondary
    from shots_goals_assists as sga
    group by 1, 2, 3
--order by sum(case when sga.event_type = 'goal' and sga.player_role = 'scorer' then 1 else 0 end) desc
)

-- #return: combine boxscore_stats & onice_stats at the player-season-game_type level of granularity
select
    /* primary key */
    {{ dbt_utils.surrogate_key(['boxscore_stats.player_id', 'boxscore_stats.season_id', 'boxscore_stats.game_type']) }} as player_season_gametype_id
    /* foreign keys */
    , boxscore_stats.player_id
    , boxscore_stats.season_id
    , boxscore_stats.game_type
    /* season properties */
    , boxscore_stats.game_type_description
    , boxscore_stats.regular_season_start_date
    , boxscore_stats.regular_season_end_date
    , boxscore_stats.full_season_games
    , boxscore_stats.boxscore_games
    /* player properties */
    , boxscore_stats.player_full_name
    /* on-ice player stats */
    -- time on ice
    , (boxscore_stats.time_on_ice_mins * 60) + (boxscore_stats.time_on_ice_seconds) as time_on_ice_seconds
    , round((boxscore_stats.time_on_ice_mins) + (boxscore_stats.time_on_ice_seconds / 60), 2) as time_on_ice_minutes
    , round(((boxscore_stats.time_on_ice_mins) + (boxscore_stats.time_on_ice_seconds / 60)) / boxscore_stats.boxscore_games, 2) as avg_time_on_ice_mins
    -- goal-scoring skater events (goals, assists, points)
    , boxscore_stats.goals
    , (boxscore_stats.goals / boxscore_stats.boxscore_games) as goals_pergame
    , ogs.goals_gamewinning
    , ogs.goals_chasegoal
    , ogs.goals_gametying
    , ogs.goals_goahead
    , ogs.goals_buffergoal
    , boxscore_stats.assists
    , ogs.assists_primary
    , ogs.assists_secondary
    , boxscore_stats.goals + boxscore_stats.assists as points
    , ((boxscore_stats.goals + boxscore_stats.assists) / boxscore_stats.boxscore_games) as points_pergame
    -- on-ice shot calculations: fenwick, corsi, shots-on-goal (goals + saves), & goals
    , oss.shots_ff
    , oss.shots_fa
    , oss.shots_cf
    , oss.shots_ca
    , oss.shots_sf
    , oss.shots_sa
    , oss.shots_gf
    , oss.shots_ga
    -- individual (i) shot calculations: shots-on-goal, fenwick-for, & corsi-for
    , oss.shots_isog
    , oss.shots_iff
    , oss.shots_icf
    , oss.shots_iblocked
    , oss.shots_imissed
    , oss.shots_isaved
    , oss.shots_iscored
    -- individual shot calculations: (for shots on net) broken down by shot type, & shot results ... exclude rebounds from agg. calculations
    ----- all individual shot types (corsi)
    , oss.shots_backhand_all
    , oss.shots_deflected_all
    , oss.shots_slapshot_all
    , oss.shots_snapshot_all
    , oss.shots_tipin_all
    , oss.shots_wraparound_all
    , oss.shots_wristshot_all
    , oss.shots_rebound_all
    ----- shot-saved (shots on-goal that were saved)
    , oss.shots_backhand_saved
    , oss.shots_deflected_saved
    , oss.shots_slapshot_saved
    , oss.shots_snapshot_saved
    , oss.shots_tipin_saved
    , oss.shots_wraparound_saved
    , oss.shots_wristshot_saved
    , oss.shots_rebound_saved
    ----- shot-goal (shots on-goal that were goals)
    , oss.shots_backhand_goal
    , oss.shots_deflected_goal
    , oss.shots_slapshot_goal
    , oss.shots_snapshot_goal
    , oss.shots_tipin_goal
    , oss.shots_wraparound_goal
    , oss.shots_wristshot_goal
    , oss.shots_rebound_goal
    ---- on-ice pcnt (%) shooting
    , case when (oss.shots_ff + oss.shots_fa) < 1 then 0 else round(100 * (oss.shots_ff / (oss.shots_ff + oss.shots_fa)), 2) end as pcnt_ff
    , case when (oss.shots_cf + oss.shots_ca) < 1 then 0 else round(100 * (oss.shots_cf / (oss.shots_cf + oss.shots_ca)), 2) end as pcnt_cf
    , case when (oss.shots_sf + oss.shots_sa) < 1 then 0 else round(100 * (oss.shots_sf / (oss.shots_sf + oss.shots_sa)), 2) end as pcnt_sf
    , case when (oss.shots_gf + oss.shots_ga) < 1 then 0 else round(100 * (oss.shots_gf / (oss.shots_gf + oss.shots_ga)), 2) end as pcnt_gf
    ---- individual (i) on-ice pcnt (%) shooting
    ---- #todo filter to a specific number of shots to be in consideration
    , case when (oss.shots_icf) < 1 then 0 else round(100 * (boxscore_stats.goals / oss.shots_icf), 2) end as pcnt_shooting_all
    --, case when (oss.shots_icf) < 1 then 0 else round((boxscore_stats.goals / oss.shots_isog), 2) end as pcnt_shooting_sog
    , case when boxscore_stats.shots < 1 then 0 else round(100 * (boxscore_stats.goals / boxscore_stats.shots), 2) end as pcnt_shooting_ongoal
    , case when oss.shots_backhand_all < 1 then 0 else round(100 * (oss.shots_backhand_goal / oss.shots_backhand_all), 2) end as pcnt_shooting_backhand
    , case when oss.shots_deflected_all < 1 then 0 else round(100 * (oss.shots_deflected_goal / oss.shots_deflected_all), 2) end as pcnt_shooting_deflected
    , case when oss.shots_slapshot_all < 1 then 0 else round(100 * (oss.shots_slapshot_goal / oss.shots_slapshot_all), 2) end as pcnt_shooting_slapshot
    , case when oss.shots_snapshot_all < 1 then 0 else round(100 * (oss.shots_snapshot_goal / oss.shots_snapshot_all), 2) end as pcnt_shooting_snapshot
    , case when oss.shots_tipin_all < 1 then 0 else round(100 * (oss.shots_tipin_goal / oss.shots_tipin_all), 2) end as pcnt_shooting_tipin
    , case when oss.shots_wraparound_all < 1 then 0 else round(100 * (oss.shots_wraparound_goal / oss.shots_wraparound_all), 2) end as pcnt_shooting_wraparound
    , case when oss.shots_wristshot_all < 1 then 0 else round(100 * (oss.shots_wristshot_goal / oss.shots_wristshot_all), 2) end as pcnt_shooting_wristshot
    , case when oss.shots_rebound_all < 1 then 0 else round(100 * (oss.shots_rebound_goal / oss.shots_rebound_all), 2) end as pcnt_shooting_rebound
    -- other skater events
    , boxscore_stats.faceoff_wins
    , boxscore_stats.faceoff_taken
    , case when boxscore_stats.faceoff_taken < 1 then 0 else round(100 * (boxscore_stats.faceoff_wins / boxscore_stats.faceoff_taken), 2) end as pcnt_faceoffwins
    , boxscore_stats.hits
    , boxscore_stats.takeaways
    , boxscore_stats.giveaways
    , boxscore_stats.blocked
    , boxscore_stats.plus_minus
    , boxscore_stats.pim
    -- special teams skater events
    , boxscore_stats.powerplay_goals
    , boxscore_stats.powerplay_assists
    , boxscore_stats.short_handed_goals
    , boxscore_stats.short_handed_assists
    -- goalie events
    , boxscore_stats.saves
    , boxscore_stats.powerplay_saves
    , boxscore_stats.even_saves
    , boxscore_stats.shorthanded_shots_against
    , boxscore_stats.even_shots_against
    , boxscore_stats.powerplay_shots_against
--,boxscore_stats.wins
--,boxscore_stats.losses
from boxscore_stats
left join onice_shots_stats as oss
    on boxscore_stats.player_id = oss.shot_player_id
        and boxscore_stats.season_id = oss.season_id
        and boxscore_stats.game_type = oss.game_type
left join onice_goals_stats as ogs
    on boxscore_stats.player_id = ogs.player_id
        and boxscore_stats.season_id = ogs.season_id
        and boxscore_stats.game_type = ogs.game_type
order by
    boxscore_stats.goals desc
