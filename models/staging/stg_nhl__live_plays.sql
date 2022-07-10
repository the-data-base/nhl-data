with

-- CTE1
live_plays as (
    select * from {{ source('meltano', 'live_plays') }}
)

-- CTE2 Play-level information (each row is a player's involvement in a play)
, cte_base_plays as (
    select
        {{ dbt_utils.surrogate_key(['live_plays.gameid', 'live_plays.about.eventidx', 'players.player.id', 'players.playertype']) }} as stg_nhl__live_plays_id
        , live_plays.gameid as game_id
        , live_plays.about.eventid as event_id
        , players.player.id as player_id
        , players.player.fullname as player_full_name
        , offset as player_index -- noqa: disable=L027
        , boxscore_player.team_id as team_id -- noqa: enable=L027
        , upper(players.playertype) as player_role
        , upper(live_plays.result.secondarytype) as event_secondary_type
        , upper(live_plays.result.penaltyseverity) as penalty_severity
        , live_plays.result.penaltyminutes as penalty_minutes
        -- Was the play/player in question home or away?
        , case
            when live_plays.team.id = schedule.away_team_id
                then 'AWAY'
            else 'HOME'
        end as player_role_team
        -- Get mins elapsed, carrying over the period
        , case
            when lower(live_plays.about.periodtype) <> 'shootout'
                then cast((substr(live_plays.about.periodtime, 0, 2)) as int64) + (20 * (cast(live_plays.about.period as int64) - 1))
        end as play_minutes_elapsed
        -- Get seconds elapsed,do not carry over the period
        , case
            when lower(live_plays.about.periodtype) <> 'shootout'
                then cast((substr(live_plays.about.periodtime, 4, 2)) as int64)
        end as play_seconds_elapsed
        , live_plays.about.eventidx as event_idx
        , live_plays.result.eventtypeid as event_type
        , live_plays.result.eventcode as event_code
        , live_plays.result.description as event_description
        , live_plays.coordinates.x as play_x_coordinate
        , live_plays.coordinates.y as play_y_coordinate
        , live_plays.about.period as play_period
        , live_plays.about.periodtype as play_period_type
        , live_plays.about.periodtime as play_period_time_elapsed
        , live_plays.about.periodtimeremaining as play_period_time_remaining
        , live_plays.about.datetime as play_time
        -- BEGIN CUMULATIVE COUNTERS BY HOME/AWAY
        -- SHOTS
        , case
            when live_plays.result.eventtypeid in ('SHOT', 'GOAL')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'GOALIE'
                then 1
            else 0
        end as shot_away
        , case
            when live_plays.result.eventtypeid in ('SHOT', 'GOAL')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'GOALIE'
                then 1
            else 0
        end as shot_home
        -- HITS
        , case
            when live_plays.result.eventtypeid in ('HIT')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'HITTER'
                then 1
            else 0
        end as hit_away
        , case
            when live_plays.result.eventtypeid in ('HIT')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'HITTER'
                then 1
            else 0
        end as hit_home
        -- FACEOFFS
        , case
            when live_plays.result.eventtypeid in ('FACEOFF')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'WINNER'
                then 1
            else 0
        end as faceoff_away
        , case
            when live_plays.result.eventtypeid in ('FACEOFF')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'WINNER'
                then 1
            else 0
        end as faceoff_home
        -- TAKEAWAYS
        , case
            when live_plays.result.eventtypeid in ('TAKEAWAY')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'PLAYERID'
                then 1
            else 0
        end as takeaway_away
        , case
            when live_plays.result.eventtypeid in ('TAKEAWAY')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'PLAYERID'
                then 1
            else 0
        end as takeaway_home
        -- GIVEAWAY
        , case
            when live_plays.result.eventtypeid in ('GIVEAWAY')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'PLAYERID'
                then 1
            else 0
        end as giveaway_away
        , case
            when live_plays.result.eventtypeid in ('GIVEAWAY')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'PLAYERID'
                then 1
            else 0
        end as giveaway_home
        -- MISSED SHOT
        , case
            when live_plays.result.eventtypeid in ('MISSED_SHOT')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'SHOOTER'
                then 1
            else 0
        end as missedshot_away
        , case
            when live_plays.result.eventtypeid in ('MISSED_SHOT')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'SHOOTER'
                then 1
            else 0
        end as missedshot_home
        -- BLOCKED SHOT
        , case
            when live_plays.result.eventtypeid in ('BLOCKED_SHOT')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'SHOOTER'
                then 1
            else 0
        end as blockedshot_away
        , case
            when live_plays.result.eventtypeid in ('BLOCKED_SHOT')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'SHOOTER'
                then 1
            else 0
        end as blockedshot_home
        -- PENALTIES
        , case
            when live_plays.result.eventtypeid in ('PENALTY')
                and live_plays.team.id = schedule.away_team_id
                and upper(players.playertype) = 'PENALTYON'
                then 1
            else 0
        end as penalty_away
        , case
            when live_plays.result.eventtypeid in ('PENALTY')
                and live_plays.team.id = schedule.home_team_id
                and upper(players.playertype) = 'PENALTYON'
                then 1
            else 0
        end as penalty_home
        -- GOALS
        , live_plays.about.goals.away as goals_away
        , live_plays.about.goals.home as goals_home

    from live_plays
    , unnest(live_plays.players) as players with offset
    left join {{ ref('stg_nhl__schedule') }} as schedule on schedule.game_id = live_plays.gameid
    left join {{ ref('stg_nhl__boxscore_player') }} as boxscore_player on boxscore_player.game_id = live_plays.gameid and players.player.id = boxscore_player.player_id
)

-- Add in cumulative metrics
, cte_cumulative as (
    select
        /* Primary Key */
        bp.stg_nhl__live_plays_id

        /* Identifiers */
        , bp.game_id
        , bp.event_idx
        , bp.event_id
        , bp.player_id
        , bp.team_id

        /* Properties */
        , bp.player_full_name
        , bp.player_index
        , upper(bp.player_role) = 'ASSIST' and player_index = 1 as player_primary_assist
        , upper(bp.player_role) = 'ASSIST' and player_index = 2 as player_secondary_assist
        , bp.player_role
        , bp.player_role_team
        , bp.event_type
        , bp.event_code
        , bp.event_description
        , bp.event_secondary_type
        , bp.penalty_severity
        , bp.penalty_minutes
        , bp.play_x_coordinate
        , bp.play_y_coordinate
        , bp.play_period
        , bp.play_period_type
        , bp.play_period_time_elapsed
        , bp.play_period_time_remaining
        -- Total seconds elapsed
        , ((bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as play_total_seconds_elapsed
        , bp.play_time
        -- Count cumulative shot totals
        , sum(bp.shot_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as shots_away
        , sum(bp.shot_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as shots_home
        -- Count hit totals
        , sum(bp.hit_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as hits_away
        , sum(bp.hit_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as hits_home
        -- Count faceoff totals
        , sum(bp.faceoff_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as faceoffs_away
        , sum(bp.faceoff_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as faceoffs_home
        -- Count takeaways totals
        , sum(bp.takeaway_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as takeaways_away
        , sum(bp.takeaway_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as takeaways_home
        -- Count giveaways totals
        , sum(bp.giveaway_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as giveaways_away
        , sum(bp.giveaway_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as giveaways_home
        -- Count missedshot totals
        , sum(bp.missedshot_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as missedshots_away
        , sum(bp.missedshot_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as missedshots_home
        -- Count blockedshot totals
        , sum(bp.blockedshot_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as blockedshots_away
        , sum(bp.blockedshot_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as blockedshots_home
        -- Count penalty totals
        , sum(bp.penalty_away) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as penalties_away
        , sum(bp.penalty_home) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as penalties_home
        -- Cumulative goal counts
        , bp.goals_away
        , bp.goals_home
        -- Cumulative goal descriptors (current state)
        , abs(bp.goals_home - bp.goals_away) as goal_difference_current
        , case
            when(bp.goals_home - bp.goals_away) = 0
                then 'Tie'
            when(bp.goals_home - bp.goals_away) > 0
                then 'Home'
            when(bp.goals_home - bp.goals_away) < 0
                then 'Away'
        end as winning_team_current
        , case
            when abs(bp.goals_home - bp.goals_away) = 0
                then 'Tie'
            when abs(bp.goals_home - bp.goals_away) = 1
                then 'Close'
            when abs(bp.goals_home - bp.goals_away) = 2
                then 'Buffer'
            when abs(bp.goals_home - bp.goals_away) = 3
                then 'Comfortable'
            when abs(bp.goals_home - bp.goals_away) >= 4
                then 'Blowout'
        end as game_state_current
        -- First goal flag
        , case
            when min(if(bp.event_type = 'GOAL', bp.event_idx, null)) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed) rows between unbounded preceding and unbounded following) = bp.event_idx
                then 1
            else 0
        end as first_goal_scored
        -- Last goal flag
        , case
            when max(if(bp.event_type = 'GOAL', bp.event_idx, null)) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed) rows between unbounded preceding and unbounded following) = bp.event_idx
                then 1
            else 0
        end as last_goal_scored
        -- Cumulative goal descriptors (previous state)
        , case
            when lag(bp.event_idx, 1) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_home, 1) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 2) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_home, 2) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 3) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_home, 3) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 4) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_home, 4) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 5) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_home, 5) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
        end as goals_home_lag
        , case
            when lag(bp.event_idx, 1) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_away, 1) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 2) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_away, 2) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 3) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_away, 3) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 4) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_away, 4) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
            when lag(bp.event_idx, 5) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) <> bp.event_idx
                then lag(bp.goals_away, 5) over (partition by game_id order by bp.game_id, event_idx, (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed))
        end as goals_away_lag

    from cte_base_plays as bp
)

-- CTE to determine the state of the game as a result of the play
, cte_game_state as (
    select
        c.*
        -- Previous winning team
        , abs(c.goals_home_lag - c.goals_away_lag) as goal_difference_lag
        , case
            when(c.goals_home_lag - c.goals_away_lag) = 0
                then 'Tie'
            when(c.goals_home_lag - c.goals_away_lag) > 0
                then 'Home'
            when(c.goals_home_lag - c.goals_away_lag) < 0
                then 'Away'
        end as winning_team_lag
        -- Previous game state
        , case
            when abs(c.goals_home_lag - c.goals_away_lag) = 0
                then 'Tie'
            when abs(c.goals_home_lag - c.goals_away_lag) = 1
                then 'Close'
            when abs(c.goals_home_lag - c.goals_away_lag) = 2
                then 'Buffer'
            when abs(c.goals_home_lag - c.goals_away_lag) = 3
                then 'Comfortable'
            when abs(c.goals_home_lag - c.goals_away_lag) >= 4
                then 'Blowout'
        end as game_state_lag
        -- Home - what was the result of the play on score?
        , case
            when c.goal_difference_current = abs(c.goals_home_lag - c.goals_away_lag)
                then 'No change'
            when c.goals_home > c.goals_home_lag
                and c.goals_home < c.goals_away
                then 'Chase goal'
            when c.goals_home > c.goals_home_lag
                and c.goals_home = c.goals_away
                then 'Tying goal scored'
            when c.goals_home < c.goals_home_lag
                and c.goals_home = c.goals_away
                then 'Tying goal allowed'
            when c.goals_home > c.goals_home_lag
                and c.goals_home_lag = c.goals_away_lag
                and c.goals_home > c.goals_away
                then 'Go-ahead goal scored'
            when c.goals_home > c.goals_home_lag
                and c.goals_home_lag > c.goals_away_lag
                and c.goals_home > c.goals_away
                then 'Buffer goal'
            when c.goals_away > c.goals_away_lag
                and c.goals_away_lag = c.goals_home_lag
                and c.goals_away > c.goals_home
                then 'Go-ahead goal allowed'
        end as home_result_of_play
        -- Away - what was the result of the play on score?
        , case
            when c.goal_difference_current = abs(c.goals_home_lag - c.goals_away_lag)
                then 'No change'
            when c.goals_away > c.goals_away_lag
                and c.goals_away < c.goals_home
                then 'Chase goal'
            when c.goals_away > c.goals_away_lag
                and c.goals_away = c.goals_home
                then 'Tying goal scored'
            when c.goals_home > c.goals_home_lag
                and c.goals_home = c.goals_away
                then 'Tying goal allowed'
            when c.goals_away > c.goals_away_lag
                and c.goals_away_lag = c.goals_home_lag
                and c.goals_away > c.goals_home
                then 'Go-ahead goal scored'
            when c.goals_away > c.goals_away_lag
                and c.goals_away_lag > c.goals_home_lag
                and c.goals_away > c.goals_home
                then 'Buffer goal'
            when c.goals_home > c.goals_home_lag
                and c.goals_home_lag = c.goals_away_lag
                and c.goals_home > c.goals_away
                then 'Go-ahead goal allowed'
        end as away_result_of_play
        -- Either team - last goal a game winning goal?
        , case
            when c.last_goal_scored = 1                       -- last goal
                and abs(c.goals_away_lag - c.goals_home_lag) = 0  -- game was tied last play
                and abs(c.goals_away - c.goals_home) <> 0         -- game no longer tied
                then 1
            else 0
        end as last_goal_game_winning
        -- Either team - last goal game tying?
        -- #TODO this is not working as intended - goals that tied the games go to OT, and so there is 0 game tying goals with this logic
        , case
            when c.last_goal_scored = 1                       -- last goal
                and abs(c.goals_away_lag - c.goals_home_lag) = 1  -- game was within 1 goal last play
                and abs(c.goals_away - c.goals_home) = 0          -- game now tied
                then 1
            else 0
        end as last_goal_game_tying

    from cte_cumulative as c

)

-- Final return
select
    /* Primary Key */
    stg_nhl__live_plays_id

    /* Identifiers */
    , game_id
    , event_idx
    , event_id
    , player_id
    , team_id

    /* Properties */
    , player_full_name
    , player_index
    , player_primary_assist
    , player_secondary_assist
    , player_role
    , player_role_team
    , event_type
    , event_code
    , event_description
    , event_secondary_type
    , penalty_severity
    , penalty_minutes
    , play_x_coordinate
    , play_y_coordinate
    , play_period
    , play_period_type
    , play_period_time_elapsed
    , play_period_time_remaining
    , play_total_seconds_elapsed
    , play_time
    , shots_away
    , shots_home
    , hits_away
    , hits_home
    , faceoffs_away
    , faceoffs_home
    , takeaways_away
    , takeaways_home
    , giveaways_away
    , giveaways_home
    , missedshots_away
    , missedshots_home
    , blockedshots_away
    , blockedshots_home
    , penalties_away
    , penalties_home
    , first_goal_scored
    , last_goal_scored
    , goals_away
    , goals_home
    , goal_difference_current
    , winning_team_current
    , game_state_current
    , home_result_of_play
    , away_result_of_play
    , last_goal_game_winning
    , last_goal_game_tying
    , goals_home_lag
    , goals_away_lag
    , goal_difference_lag
    , winning_team_lag
    , game_state_lag

from cte_game_state
