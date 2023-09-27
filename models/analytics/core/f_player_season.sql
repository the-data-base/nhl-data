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
        , sum(cast(split(bp.time_on_ice, ':')[offset(0)] as int)) as time_on_ice_mins_raw
        , sum(cast(split(bp.time_on_ice, ':')[offset(1)] as int)) as time_on_ice_seconds_raw
        , (sum(cast(split(bp.time_on_ice, ':')[offset(0)] as int)) * 60) + sum(cast(split(bp.time_on_ice, ':')[offset(1)] as int)) as time_on_ice_seconds
        , sum(cast(split(bp.time_on_ice, ':')[offset(0)] as int)) + (sum(cast(split(bp.time_on_ice, ':')[offset(1)] as int)) / 60) as time_on_ice_minutes
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

    where
        1 = 1
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
        , plays.last_goal_game_winning
        , plays.player_primary_assist
        , plays.player_secondary_assist
        , lower(schedule.game_type_description) as game_type_description
        , lower(plays.event_description) as event_description
        , lower(plays.event_type) as event_type
        , lower(plays.event_secondary_type) as event_secondary_type
        , lower(plays.player_role_team) as player_role_team
        , lower(plays.player_role) as player_role
        , lower(plays.play_period_type) as play_period_type
        , case when plays.play_period > 3 then 1 else 0 end as overtime
        , lower(plays.home_result_of_play) as home_result_of_play
        , lower(plays.away_result_of_play) as away_result_of_play
        , plays.home_skaters
        , plays.away_skaters
        , plays.seconds_since_last_shot
        , plays.shot_rebound_ind
        , plays.home_goalie_pulled
        , plays.away_goalie_pulled
        , case
            when (lower(substr(plays.last_play_event_secondary_type, 0, 4)) = 'ps -') and (lower(plays.event_type) in ('shot', 'goal', 'missed_shot')) then 1
            else 0
        end as penalty_shot_attempt
        , plays.last_shot_seconds
        , plays.last_shot_rebound_ind
        , plays.xg_fenwick_shot
        , plays.x_goal
        , plays.xg_model_id
        , plays.xg_strength_state_code
        , plays.xg_proba
    from {{ ref('f_plays') }} as plays
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = plays.game_id
    left join {{ ref('d_seasons') }} as season on season.season_id = schedule.season_id
    left join {{ ref('d_teams') }} as team on team.team_id = plays.team_id
    where 1 = 1
    -- remove shootout plays
    and lower(plays.play_period_type) != 'shootout'
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
        , home_sga.player_role_team as shooting_team
        , home_sga.player_id as shooter_player_id
        , 'home' as skater_type
    from shots_goals_assists as home_sga
    , unnest(home_sga.home_skaters) as shot_player_id
    where home_sga.player_role in ('shooter', 'scorer')
    union all
    select
        *
        , away_sga.player_role_team as shooting_team
        , away_sga.player_id as shooter_player_id
        , 'away' as skater_type
    from shots_goals_assists as away_sga
    , unnest(away_sga.away_skaters) as shot_player_id
    where away_sga.player_role in ('shooter', 'scorer')
)

-- cte#4: on-ice shots stats - shot & goal summaries
, shots_involvement as (
    select
        player.full_name as player_full_name
        , s.shot_player_id
        , s.game_id
        , s.game_type
        , s.season_id
        -- shot descriptors
        , case when s.shooting_team = s.skater_type then 'shot for' else 'shot against' end as shot_type
        , case
            when s.shooter_player_id = s.shot_player_id then 'shooter'
            when s.shooting_team = s.skater_type then 'shooter teammate'
            when s.shooting_team != s.skater_type then 'shooter opponent'
        end as shooter_description
        , s.shooting_team
        , s.shooter_player_id
        , s.skater_type
        , s.event_type
        , s.event_secondary_type
        , s.shot_rebound_ind as shots_rebound
        , s.last_shot_rebound_ind as last_shot_rebound
        , s.penalty_shot_attempt
        , case
            when (s.event_type = 'goal') and (s.skater_type = 'home') and (s.shooter_player_id = s.shot_player_id) and (s.away_goalie_pulled is true) then 1
            when (s.event_type = 'goal') and (s.skater_type = 'away') and (s.shooter_player_id = s.shot_player_id) and (s.home_goalie_pulled is true) then 1
            else 0
        end as empty_net_goal
        -- shot calculations
        , case when s.event_type in ('goal', 'shot') then 1 else 0 end as shots_ongoal
        , case when s.event_type = 'blocked_shot' then 1 else 0 end as shots_blocked
        , case when s.event_type = 'missed_shot' then 1 else 0 end as shots_missed
        , case when s.event_type = 'shot' then 1 else 0 end as shots_saved
        , case when s.event_type = 'goal' then 1 else 0 end as shots_scored
        , case when s.event_type in ('blocked_shot', 'missed_shot', 'shot', 'goal') then 1 else 0 end as corsi_shot
        , case when s.event_type in ('missed_shot', 'shot', 'goal') then 1 else 0 end as fenwick_shot
        -- xg stuff
        , s.xg_fenwick_shot
        , s.x_goal
        , s.xg_model_id
        , s.xg_strength_state_code
        , s.xg_proba
    from shots_flat as s
    left join {{ ref('d_players') }} as player on player.player_id = s.shot_player_id
    where lower(player.primary_position_code) != 'g'
)

-- cte#5: on-ice shots stats - summarizing individual shots (i) and on-ice shots
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
        -- on-ice xg shot calculations: probability of a fenwick shot being a goal
        , sum(case when xg_fenwick_shot = 1 and shot_type = 'shot for' then shots_scored else 0 end) as shots_xgf
        , sum(case when xg_fenwick_shot = 1 and shot_type = 'shot against' then shots_scored else 0 end) as shots_xga
        ----- even-strength (ev)
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shot_type = 'shot for' then xg_proba else 0 end) as shots_ev_xgf
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shot_type = 'shot against' then xg_proba else 0 end) as shots_ev_xga
        ----- power-play (pp)
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shot_type = 'shot for' then xg_proba else 0 end) as shots_pp_xgf
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shot_type = 'shot against' then xg_proba else 0 end) as shots_pp_xga
        ----- short-handed (sh)
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shot_type = 'shot for' then xg_proba else 0 end) as shots_sh_xgf
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shot_type = 'shot against' then xg_proba else 0 end) as shots_sh_xga
        -- individual (i) shot calculations: shots-on-goal, fenwick-for, corsi-for & expected goals (xg)
        , sum(case when shooter_description = 'shooter' then shots_ongoal else 0 end) as shots_isog
        , sum(case when shooter_description = 'shooter' then fenwick_shot else 0 end) as shots_iff
        , sum(case when shooter_description = 'shooter' then corsi_shot else 0 end) as shots_icf
        , sum(case when shooter_description = 'shooter' then shots_blocked else 0 end) as shots_iblocked
        , sum(case when shooter_description = 'shooter' then shots_missed else 0 end) as shots_imissed
        , sum(case when shooter_description = 'shooter' then shots_saved else 0 end) as shots_isaved
        , sum(case when shooter_description = 'shooter' then shots_scored else 0 end) as shots_iscored
        , sum(case when xg_fenwick_shot = 1 and shooter_description = 'shooter' then xg_proba else 0 end) as shots_ixg
        -- individual shot calculations: (for shots on net) broken down by strength state, & shot results (cannot use corsi or blocked as xg_strength_state_code excludes blocked shots (fenwick only))
        ----- even-strength (ev)
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then shots_ongoal else 0 end) as shots_ev_isog
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then fenwick_shot else 0 end) as shots_ev_iff
        --, sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_ev_icf
        --, sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then shots_blocked else 0 end) as shots_ev_iblocked
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then shots_missed else 0 end) as shots_ev_imissed
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_ev_isaved
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_ev_iscored
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'ev' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_ev_ixg
        ----- power-play (pp)
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then shots_ongoal else 0 end) as shots_pp_isog
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then fenwick_shot else 0 end) as shots_pp_iff
        --, sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_pp_icf
        --, sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then shots_blocked else 0 end) as shots_pp_iblocked
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then shots_missed else 0 end) as shots_pp_imissed
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_pp_isaved
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_pp_iscored
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'pp' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_pp_ixg
        ----- short-handed (sh)
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then shots_ongoal else 0 end) as shots_sh_isog
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then fenwick_shot else 0 end) as shots_sh_iff
        --, sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then corsi_shot else 0 end) as shots_sh_icf
        --, sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then shots_blocked else 0 end) as shots_sh_iblocked
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then shots_missed else 0 end) as shots_sh_imissed
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then shots_saved else 0 end) as shots_sh_isaved
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then shots_scored else 0 end) as shots_sh_iscored
        , sum(case when xg_fenwick_shot = 1 and xg_strength_state_code = 'sh' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_sh_ixg
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
        ----- xg (expected goals by shot type)
        , sum(case when xg_fenwick_shot = 1 and event_secondary_type = 'backhand' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_backhand_xg
        , sum(case when xg_fenwick_shot = 1 and event_secondary_type = 'deflected' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_deflected_xg
        , sum(case when xg_fenwick_shot = 1 and event_secondary_type = 'slap shot' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_slapshot_xg
        , sum(case when xg_fenwick_shot = 1 and event_secondary_type = 'snap shot' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_snapshot_xg
        , sum(case when xg_fenwick_shot = 1 and event_secondary_type = 'tip-in' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_tipin_xg
        , sum(case when xg_fenwick_shot = 1 and event_secondary_type = 'wrap-around' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_wraparound_xg
        , sum(case when xg_fenwick_shot = 1 and event_secondary_type = 'wrist shot' and shooter_description = 'shooter' then xg_proba else 0 end) as shots_wristshot_xg
        , sum(case when xg_fenwick_shot = 1 and shots_rebound = 1 and shooter_description = 'shooter' then xg_proba else 0 end) as shots_rebound_xg
        -- lastly... specialty cases (penalty shots & empty net)
        , sum(case when shot_type = 'shot for' then penalty_shot_attempt else 0 end) as penalty_shot_attempts
        , sum(case when shot_type = 'shot for' and shots_scored = 1 then penalty_shot_attempt else 0 end) as penalty_shot_goals
        , sum(case when shot_type = 'shot for' then empty_net_goal else 0 end) as empty_net_goals
    from shots_involvement
    group by
        game_type
        , season_id
        , shot_player_id
        , player_full_name
)

-- cte#6: on-ice goal stats: goal & assist summaries
, onice_goals_stats as (
    select
        sga.player_id
        , sga.season_id
        , sga.game_type
        , max(sga.event_description) as example_eventdescription
        -- goal types
        , sum(case when sga.overtime = 1 and sga.player_role = 'scorer' then 1 else 0 end) as goals_overtime
        , sum(case when sga.last_goal_game_winning = 1 and sga.player_role = 'scorer' then 1 else 0 end) as goals_gamewinning
        , sum(case when (sga.home_result_of_play = 'chase goal' or sga.away_result_of_play = 'chase goal') and sga.player_role = 'scorer' then 1 else 0 end) as goals_chasegoal
        , sum(case when (sga.home_result_of_play = 'tying goal scored' or sga.away_result_of_play = 'tying goal scored') and sga.player_role = 'scorer' then 1 else 0 end) as goals_gametying
        , sum(case when (sga.home_result_of_play = 'go-ahead goal scored' or sga.away_result_of_play = 'go-ahead goal scored') and sga.player_role = 'scorer' then 1 else 0 end) as goals_goahead
        , sum(case when (sga.home_result_of_play = 'buffer goal' or sga.away_result_of_play = 'buffer goal') and sga.player_role = 'scorer' then 1 else 0 end) as goals_buffergoal
        -- assist types
        , sum(case when sga.player_primary_assist = true and sga.player_role = 'assist' and sga.event_type = 'goal' then 1 else 0 end) as assists_primary
        , sum(case when sga.player_secondary_assist = true and sga.player_role = 'assist' and sga.event_type = 'goal' then 1 else 0 end) as assists_secondary
    from shots_goals_assists as sga
    group by
        sga.player_id
        , sga.season_id
        , sga.game_type
)

-- cte#7: summarizes penalty mins at the player-season-game_type level
, penalty_stats as (
    select
        plays.player_id
        , season.season_id
        , schedule.game_type
        , sum(case when lower(plays.penalty_severity) = 'minor' and lower(plays.player_role) = 'penaltyon' then plays.penalty_minutes else 0 end) as minor_pim_taken
        , sum(case when lower(plays.penalty_severity) = 'minor' and lower(plays.player_role) = 'drewby' then plays.penalty_minutes else 0 end) as minor_pim_drawn
        , sum(case when lower(plays.penalty_severity) = 'major' and lower(plays.player_role) = 'penaltyon' then plays.penalty_minutes else 0 end) as major_pim_taken
        , sum(case when lower(plays.penalty_severity) = 'major' and lower(plays.player_role) = 'drewby' then plays.penalty_minutes else 0 end) as major_pim_drawn
    from {{ ref('f_plays') }} as plays
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = plays.game_id
    left join {{ ref('d_seasons') }} as season on season.season_id = schedule.season_id
    where lower(plays.event_type) = 'penalty'
    group by
        plays.player_id
        , season.season_id
        , schedule.game_type
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
    -- time on ice
    --, (boxscore_stats.time_on_ice_mins_raw * 60) + (boxscore_stats.time_on_ice_seconds_raw) as time_on_ice_seconds
    --, round((boxscore_stats.time_on_ice_mins_raw) + (boxscore_stats.time_on_ice_seconds_raw / 60), 2) as time_on_ice_minutes
    , boxscore_stats.time_on_ice_seconds
    , round(boxscore_stats.time_on_ice_minutes, 2) as time_on_ice_minutes
    , round(boxscore_stats.time_on_ice_minutes / boxscore_stats.boxscore_games, 2) as avg_time_on_ice_mins
    , ps.minor_pim_drawn
    , ps.minor_pim_taken
    , ps.major_pim_drawn
    , ps.major_pim_taken
    -- goal-scoring skater events (goals, assists, points)
    , boxscore_stats.goals
    , ogs.goals_overtime
    , ogs.goals_gamewinning
    , ogs.goals_chasegoal
    , ogs.goals_gametying
    , ogs.goals_goahead
    , ogs.goals_buffergoal
    , boxscore_stats.assists
    , ogs.assists_primary
    , ogs.assists_secondary
    , boxscore_stats.goals + boxscore_stats.assists as points
    , case when boxscore_stats.boxscore_games > 0 then round(boxscore_stats.goals / boxscore_stats.boxscore_games, 4) end as goals_pergame
    , case when boxscore_stats.boxscore_games > 0 then round(boxscore_stats.assists / boxscore_stats.boxscore_games, 4) end as assists_pergame
    , case when boxscore_stats.boxscore_games > 0 then round(ogs.assists_primary / boxscore_stats.boxscore_games, 4) end as assists_primary_pergame
    , case when boxscore_stats.boxscore_games > 0 then round(ogs.assists_secondary / boxscore_stats.boxscore_games, 4) end as assists_secondary_pergame
    , case when boxscore_stats.boxscore_games > 0 then round((boxscore_stats.goals + boxscore_stats.assists) / boxscore_stats.boxscore_games, 4) end as points_pergame
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(boxscore_stats.goals / (boxscore_stats.time_on_ice_minutes / 60), 4) end as goals_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(boxscore_stats.assists / (boxscore_stats.time_on_ice_minutes / 60), 4) end as assists_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(ogs.assists_primary / (boxscore_stats.time_on_ice_minutes / 60), 4) end as asissts_primary_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(ogs.assists_secondary / (boxscore_stats.time_on_ice_minutes / 60), 4) end as assists_secondary_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round((boxscore_stats.goals + boxscore_stats.assists) / (boxscore_stats.time_on_ice_minutes / 60), 4) end as points_per60
    -- on-ice shot calculations: fenwick, corsi, shots-on-goal (goals + saves), & goals
    , boxscore_stats.shots as shots
    , oss.shots_ff
    , oss.shots_fa
    , oss.shots_cf
    , oss.shots_ca
    , oss.shots_sf
    , oss.shots_sa
    , oss.shots_gf
    , oss.shots_ga
    , oss.shots_xgf
    , oss.shots_xga
    , oss.shots_ev_xgf
    , oss.shots_ev_xga
    , oss.shots_pp_xgf
    , oss.shots_pp_xga
    , oss.shots_sh_xgf
    , oss.shots_sh_xga
    -- individual (i) shot calculations: shots-on-goal, fenwick-for, & corsi-for
    , oss.shots_isog
    , oss.shots_iff
    , oss.shots_icf
    , oss.shots_iblocked
    , oss.shots_imissed
    , oss.shots_isaved
    , oss.shots_iscored
    , oss.shots_ixg
    -- per-60-on-ice shot calculations: fenwick, corsi, shots-on-goal (goals + saves), & goals
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_ff / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_ff_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_fa / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_fa_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_cf / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_cf_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_ca / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_ca_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_sf / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_sf_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_sa / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_sa_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_gf / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_gf_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_ga / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_ga_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_xgf / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_xgf_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_xga / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_xga_per60
    -- per-60-individual (i) shot calculations: shots-on-goal, fenwick-for, & corsi-for
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_isog / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_isog_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_iff / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_iff_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_icf / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_icf_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_iblocked / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_iblocked_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_imissed / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_imissed_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_isaved / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_isaved_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_iscored / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_iscored_per60
    , case when boxscore_stats.time_on_ice_seconds > 0 then round(oss.shots_ixg / (boxscore_stats.time_on_ice_minutes / 60), 4) end as shots_ixg_per60
    -- individual shot calculations: (for shots on net) broken down by strength state
    ----- even-strength (ev)
    , oss.shots_ev_isog
    , oss.shots_ev_iff
    , oss.shots_ev_imissed
    , oss.shots_ev_isaved
    , oss.shots_ev_iscored
    , oss.shots_ev_ixg
    ----- power-play (pp)
    , oss.shots_pp_isog
    , oss.shots_pp_iff
    , oss.shots_pp_imissed
    , oss.shots_pp_isaved
    , oss.shots_pp_iscored
    , oss.shots_pp_ixg
    ----- power-play (pp)
    , oss.shots_sh_isog
    , oss.shots_sh_iff
    , oss.shots_sh_imissed
    , oss.shots_sh_isaved
    , oss.shots_sh_iscored
    , oss.shots_sh_ixg
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
    ----- shot-xg (sum of expected goals)
    , oss.shots_backhand_xg
    , oss.shots_deflected_xg
    , oss.shots_slapshot_xg
    , oss.shots_snapshot_xg
    , oss.shots_tipin_xg
    , oss.shots_wraparound_xg
    , oss.shots_wristshot_xg
    , oss.shots_rebound_xg
    ---- special case shots/goals
    , oss.penalty_shot_attempts
    , oss.penalty_shot_goals
    , oss.empty_net_goals
    ---- on-ice pcnt (%) shooting
    , case when (oss.shots_ff + oss.shots_fa) < 1 then 0 else round(100 * (oss.shots_ff / (oss.shots_ff + oss.shots_fa)), 2) end as pcnt_ff
    , case when (oss.shots_cf + oss.shots_ca) < 1 then 0 else round(100 * (oss.shots_cf / (oss.shots_cf + oss.shots_ca)), 2) end as pcnt_cf
    , case when (oss.shots_sf + oss.shots_sa) < 1 then 0 else round(100 * (oss.shots_sf / (oss.shots_sf + oss.shots_sa)), 2) end as pcnt_sf
    , case when (oss.shots_gf + oss.shots_ga) < 1 then 0 else round(100 * (oss.shots_gf / (oss.shots_gf + oss.shots_ga)), 2) end as pcnt_gf
    , case when (oss.shots_xgf + oss.shots_xga) < 1 then 0 else round(100 * (oss.shots_xgf / (oss.shots_xgf + oss.shots_xga)), 2) end as pcnt_xgf -- this work?
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
    on
        boxscore_stats.player_id = oss.shot_player_id
        and boxscore_stats.season_id = oss.season_id
        and boxscore_stats.game_type = oss.game_type
left join onice_goals_stats as ogs
    on
        boxscore_stats.player_id = ogs.player_id
        and boxscore_stats.season_id = ogs.season_id
        and boxscore_stats.game_type = ogs.game_type
left join penalty_stats as ps
    on
        boxscore_stats.player_id = ps.player_id
        and boxscore_stats.season_id = ps.season_id
        and boxscore_stats.game_type = ps.game_type
order by
    boxscore_stats.goals desc
