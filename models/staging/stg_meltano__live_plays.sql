with

-- CTE1
live_plays as (
    select  
        * 
    from    
        {{ source('meltano', 'live_plays') }}
    )

-- CTE2 Play-level information (each row is a player's involvement in a play)
,cte_base_plays as (
    select
        concat(live_plays.game_id, live_plays.about.eventidx, players.player.id) as id
        , live_plays.game_id
        , live_plays.about.eventid as event_id
        , players.player.id as player_id
        , boxscore_player.team_id as team_id
        , upper(players.playertype) as player_role
        -- Was the play/player in question home or away?
        , case
            when live_plays.team.id = schedule.away_team_id
                then 'AWAY'
            else 'HOME'
        end as player_role_team
        -- Get mins elapsed, carrying over the period
        , case
            when live_plays.about.periodtype = 'REGULAR'
                then cast((substr(live_plays.about.periodtime, 0, 2)) as int64) + (20 * (cast(live_plays.about.period as int64) - 1))
        end as play_minutes_elapsed
        -- Get seconds elapsed,do not carry over the period
        , case
            when live_plays.about.periodtype = 'REGULAR'
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
        --,DATETIME(live_plays.about.dateTime) as play_time
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

    from
        live_plays
        , unnest(live_plays.players) as players
        left join {{ref('stg_meltano__schedule')}} as schedule on schedule.game_id = live_plays.game_id
        left join {{ ref('stg_meltano__boxscore_player') }} as boxscore_player on boxscore_player.game_id = live_plays.game_id and players.player.id = boxscore_player.player_id
    )

-- Add in cumulative metrics
, cte_cumulative as (
    select
        /* Primary Key */
        bp.id

        /* Foreign Keys */
        , bp.game_id
        , bp.event_idx
        , bp.event_id
        , bp.player_id
        , bp.team_id

        /* Properties */
        , bp.player_role
        , bp.player_role_team
        , bp.event_type
        , bp.event_code
        , bp.event_description
        , bp.play_x_coordinate
        , bp.play_y_coordinate
        , bp.play_period
        , bp.play_period_type
        , bp.play_period_time_elapsed
        , bp.play_period_time_remaining
        , bp.play_seconds_elapsed as play_period_seconds_elapsed
        , 1200 - play_seconds_elapsed as play_period_seconds_remaining
        , ((bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as play_total_seconds_elapsed
        , 3600 - ((bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)) as play_total_seconds_remaining
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
                then 'TIE'
            when(bp.goals_home - bp.goals_away) > 0
                then 'HOME'
            when(bp.goals_home - bp.goals_away) < 0
                then 'AWAY'
        end as winning_team_current
        , case
            when abs(bp.goals_home - bp.goals_away) = 0
                then 'TIE'
            when abs(bp.goals_home - bp.goals_away) = 1
                then 'CLOSE'
            when abs(bp.goals_home - bp.goals_away) = 2
                then 'BUFFER'
            when abs(bp.goals_home - bp.goals_away) = 3
                then 'COMFORTABLE'
            when abs(bp.goals_home - bp.goals_away) >= 4
                then 'BLOWOUT'
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

    from
        cte_base_plays as bp

    order by
        bp.game_id
        , (bp.play_minutes_elapsed * 60) + (bp.play_seconds_elapsed)
        , event_idx
)

-- CTE to determine the state of the game as a result of the play
, cte_game_state as (
    select
        c.*
        -- Previous winning team
        , abs(c.goals_home_lag - c.goals_away_lag) as goal_difference_lag
        , case
            when(c.goals_home_lag - c.goals_away_lag) = 0
                then 'TIE'
            when(c.goals_home_lag - c.goals_away_lag) > 0
                then 'HOME'
            when(c.goals_home_lag - c.goals_away_lag) < 0
                then 'AWAY'
        end as winning_team_lag
        -- Previous game state
        , case
            when abs(c.goals_home_lag - c.goals_away_lag) = 0
                then 'TIE'
            when abs(c.goals_home_lag - c.goals_away_lag) = 1
                then 'CLOSE'
            when abs(c.goals_home_lag - c.goals_away_lag) = 2
                then 'BUFFER'
            when abs(c.goals_home_lag - c.goals_away_lag) = 3
                then 'COMFORTABLE'
            when abs(c.goals_home_lag - c.goals_away_lag) >= 4
                then 'BLOWOUT'
        end as game_state_lag
        -- Home - what was the result of the play on score?
        , case
            when c.goal_difference_current = abs(c.goals_home_lag - c.goals_away_lag)
                then 'NO CHANGE'
            when c.goals_home > c.goals_home_lag
                and c.goals_home < c.goals_away
                then 'CHASE GOAL'
            when c.goals_home > c.goals_home_lag
                and c.goals_home = c.goals_away
                then 'TYING GOAL SCORED'
            when c.goals_home < c.goals_home_lag
                and c.goals_home = c.goals_away
                then 'TYING GOAL ALLOWED'
            when c.goals_home > c.goals_home_lag
                and c.goals_home_lag = c.goals_away_lag
                and c.goals_home > c.goals_away
                then 'GO-AHEAD GOAL SCORED'
            when c.goals_home > c.goals_home_lag
                and c.goals_home_lag > c.goals_away_lag
                and c.goals_home > c.goals_away
                then 'BUFFER GOAL'
            when c.goals_away > c.goals_away_lag
                and c.goals_away_lag = c.goals_home_lag
                and c.goals_away > c.goals_home
                then 'GO-AHEAD GOAL ALLOWED'
        end as home_result_of_play
        -- Away - what was the result of the play on score?
        , case
            when c.goal_difference_current = abs(c.goals_home_lag - c.goals_away_lag)
                then 'NO CHANGE'
            when c.goals_away > c.goals_away_lag
                and c.goals_away < c.goals_home
                then 'CHASE GOAL'
            when c.goals_away > c.goals_away_lag
                and c.goals_away = c.goals_home
                then 'TYING GOAL SCORED'
            when c.goals_home > c.goals_home_lag
                and c.goals_home = c.goals_away
                then 'TYING GOAL ALLOWED'
            when c.goals_away > c.goals_away_lag
                and c.goals_away_lag = c.goals_home_lag
                and c.goals_away > c.goals_home
                then 'GO-AHEAD GOAL SCORED'
            when c.goals_away > c.goals_away_lag
                and c.goals_away_lag > c.goals_home_lag
                and c.goals_away > c.goals_home
                then 'BUFFER GOAL'
            when c.goals_home > c.goals_home_lag
                and c.goals_home_lag = c.goals_away_lag
                and c.goals_home > c.goals_away
                then 'GO-AHEAD GOAL ALLOWED'
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
        , case
            when c.last_goal_scored = 1                       -- last goal
                and abs(c.goals_away_lag - c.goals_home_lag) = 1  -- game was within 1 goal last play
                and abs(c.goals_away - c.goals_home) = 0          -- game now tied
                then 1
            else 0
        end as last_goal_game_tying

    from
        cte_cumulative as c

)

-- Final return
select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['g.id']) }} as id

    /* Foreign Keys */
    , g.game_id
    , g.event_idx
    , g.event_id
    , g.player_id
    , g.team_id

    /* Properties */
    , g.player_role
    , g.player_role_team
    , g.event_type
    , g.event_code
    , g.event_description
    , g.play_x_coordinate
    , g.play_y_coordinate
    , g.play_period
    , g.play_period_type
    , g.play_period_time_elapsed
    , g.play_period_time_remaining
    , g.play_period_seconds_elapsed
    , g.play_period_seconds_remaining
    , g.play_total_seconds_elapsed
    , g.play_total_seconds_remaining
    , g.play_time
    , g.shots_away
    , g.shots_home
    , g.hits_away
    , g.hits_home
    , g.faceoffs_away
    , g.faceoffs_home
    , g.takeaways_away
    , g.takeaways_home
    , g.giveaways_away
    , g.giveaways_home
    , g.missedshots_away
    , g.missedshots_home
    , g.blockedshots_away
    , g.blockedshots_home
    , g.penalties_away
    , g.penalties_home
    , g.first_goal_scored
    , g.last_goal_scored
    , g.goals_away
    , g.goals_home
    , g.goal_difference_current
    , g.winning_team_current
    , g.game_state_current
    , g.home_result_of_play
    , g.away_result_of_play
    , g.last_goal_game_winning
    , g.last_goal_game_tying
    , g.goals_home_lag
    , g.goals_away_lag
    , g.goal_difference_lag
    , g.winning_team_lag
    , g.game_state_lag

from
    cte_game_state as g
