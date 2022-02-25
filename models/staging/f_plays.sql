WITH 

-- Create f_boxscore_player (boxcore--player granularity)

-- CTE1
 CTE_AWAY_PLAYER_TEAM AS (
    
    SELECT 
        b.game_id
        ,ap.person.id as player_id
        ,b.teams.away.team.id as team_id
        ,'No' as player_home_team

    
    FROM 
        {{ref('live_boxscore')}} as b
        ,UNNEST(b.teams.away.players) as ap

  )

-- CTE2
, CTE_HOME_PLAYER_TEAM AS (
    
    SELECT 
        b.game_id
        ,hp.person.id as player_id
        ,b.teams.home.team.id as team_id
        ,'Yes' as player_home_team
    
    FROM 
        {{ref('live_boxscore')}} as b
        ,UNNEST(b.teams.home.players) as hp
)

-- CTE3
, CTE_GAME_PLAYER_TEAM AS (

    SELECT
        hpt.*

    FROM
        CTE_HOME_PLAYER_TEAM hpt

    UNION ALL

    SELECT
        apt.*
    FROM
        CTE_AWAY_PLAYER_TEAM apt

)

-- CTE: Play-level information (each row is a player's involvement in a play)
 , CTE_BASE_PLAYS AS (
    SELECT 
        CONCAT(x.game_id, "_", x.about.eventIdx, "_", p.player.id) as id
        ,x.game_id
        ,x.about.eventId as event_id
        ,p.player.id as player_id
        ,gpt.team_id as team_id
        ,UPPER(p.playerType) as player_role
        -- Was the play/player in question home or away?
        ,CASE 
            WHEN x.team.id = s.teams.away.team.id 
                THEN 'AWAY'
            ELSE 'HOME'
        END AS player_role_team 
       -- Get mins elapsed, carrying over the period
        ,CASE 
            WHEN x.about.periodType = "REGULAR"
                THEN CAST((SUBSTR(x.about.periodTime, 0, 2)) AS INT64) + (20 * (CAST(x.about.period as INT64) - 1))
            ELSE NULL
        END as play_minutes_elapsed
       -- Get seconds elapsed,do not carry over the period
        ,CASE 
            WHEN x.about.periodType = "REGULAR"
                THEN CAST((SUBSTR(x.about.periodTime, 4, 2)) AS INT64)
            ELSE NULL
        END as play_seconds_elapsed
        ,x.about.eventIdx as event_idx
        ,x.result.eventTypeId as event_type
        ,x.result.eventCode as event_code
        ,x.result.description as event_description
        ,x.coordinates.x as play_x_coordinate
        ,x.coordinates.y as play_y_coordinate
        ,x.about.period as play_period
        ,x.about.periodType as play_period_type
        ,x.about.periodTime as play_period_time_elapsed
        ,x.about.periodTimeRemaining as play_period_time_remaining
        --,DATETIME(x.about.dateTime) as play_time
        ,x.about.dateTime as play_time
    -- BEGIN CUMULATIVE COUNTERS BY HOME/AWAY
        -- SHOTS
        ,CASE 
            WHEN x.result.eventTypeId in ('SHOT', 'GOAL') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'GOALIE'
                THEN 1
            ELSE 0
        END AS shot_away
        ,CASE 
            WHEN x.result.eventTypeId in ('SHOT', 'GOAL') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'GOALIE'
                THEN 1
            ELSE 0
        END AS shot_home
        -- HITS
        ,CASE 
            WHEN x.result.eventTypeId in ('HIT') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'HITTER'
                THEN 1
            ELSE 0
        END AS hit_away
        ,CASE 
            WHEN x.result.eventTypeId in ('HIT') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'HITTER'
                THEN 1
            ELSE 0
        END AS hit_home
        -- FACEOFFS 
        ,CASE 
            WHEN x.result.eventTypeId in ('FACEOFF') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'WINNER'
                THEN 1
            ELSE 0
        END AS faceoff_away
        ,CASE 
            WHEN x.result.eventTypeId in ('FACEOFF') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'WINNER'
                THEN 1
            ELSE 0
        END AS faceoff_home
        -- TAKEAWAYS
        ,CASE 
            WHEN x.result.eventTypeId in ('TAKEAWAY') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'PLAYERID'
                THEN 1
            ELSE 0
        END AS takeaway_away
        ,CASE 
            WHEN x.result.eventTypeId in ('TAKEAWAY') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'PLAYERID'
                THEN 1
            ELSE 0
        END AS takeaway_home
        -- GIVEAWAY
        ,CASE 
            WHEN x.result.eventTypeId in ('GIVEAWAY') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'PLAYERID'
                THEN 1
            ELSE 0
        END AS giveaway_away
        ,CASE 
            WHEN x.result.eventTypeId in ('GIVEAWAY') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'PLAYERID'
                THEN 1
            ELSE 0
        END AS giveaway_home
        -- MISSED SHOT
        ,CASE 
            WHEN x.result.eventTypeId in ('MISSED_SHOT') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'SHOOTER'
                THEN 1
            ELSE 0
        END AS missedshot_away
        ,CASE 
            WHEN x.result.eventTypeId in ('MISSED_SHOT') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'SHOOTER'
                THEN 1
            ELSE 0
        END AS missedshot_home
        -- BLOCKED SHOT
        ,CASE 
            WHEN x.result.eventTypeId in ('BLOCKED_SHOT') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'SHOOTER'
                THEN 1
            ELSE 0
        END AS blockedshot_away
        ,CASE 
            WHEN x.result.eventTypeId in ('BLOCKED_SHOT') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'SHOOTER'
                THEN 1
            ELSE 0
        END AS blockedshot_home
        -- PENALTIES
        ,CASE 
            WHEN x.result.eventTypeId in ('PENALTY') 
            AND x.team.id = s.teams.away.team.id 
            AND UPPER(p.playerType) = 'PENALTYON'
                THEN 1
            ELSE 0
        END AS penalty_away
        ,CASE 
            WHEN x.result.eventTypeId in ('PENALTY') 
            AND x.team.id = s.teams.home.team.id 
            AND UPPER(p.playerType) = 'PENALTYON'
                THEN 1
            ELSE 0
        END AS penalty_home
        -- GOALS
        ,x.about.goals.away as goals_away
        ,x.about.goals.home as goals_home

    FROM 
        {{ref('live_plays')}} as x
        ,UNNEST(x.players) as p
        LEFT JOIN {{ref('schedule')}} as s on s.gamePK = x.game_id
        LEFT JOIN CTE_GAME_PLAYER_TEAM as gpt on gpt.game_id = x.game_id and gpt.player_id = p.player.id
)

-- Add in cumulative metrics
, CTE_CUMULATIVE as (
    SELECT 
        /* Primary Key */
        bp.id

        /* Foreign Keys */
        ,bp.game_id
        ,bp.event_idx
        ,bp.event_id
        ,bp.player_id
        ,bp.team_id

        /* Properties */
        ,bp.player_role
        ,bp.player_role_team 
        ,bp.event_type
        ,bp.event_code
        ,bp.event_description
        ,bp.play_x_coordinate
        ,bp.play_y_coordinate
        ,bp.play_period
        ,bp.play_period_type
        ,bp.play_period_time_elapsed
        ,bp.play_period_time_remaining
        ,bp.play_seconds_elapsed as play_period_seconds_elapsed
        ,1200 - play_seconds_elapsed as play_period_seconds_remaining
        ,((bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) as play_total_seconds_elapsed
        ,3600 - ((bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) as play_total_seconds_remaining
        ,bp.play_time
        -- Count cumulative shot totals
        , SUM(bp.shot_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS shots_away
        , SUM(bp.shot_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS shots_home
        -- Count hit totals
        , SUM(bp.hit_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS hits_away
        , SUM(bp.hit_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS hits_home
        -- Count faceoff totals
        , SUM(bp.faceoff_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS faceoffs_away
        , SUM(bp.faceoff_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS faceoffs_home
        -- Count takeaways totals
        , SUM(bp.takeaway_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS takeaways_away
        , SUM(bp.takeaway_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS takeaways_home
        -- Count giveaways totals
        , SUM(bp.giveaway_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS giveaways_away
        , SUM(bp.giveaway_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS giveaways_home
        -- Count missedshot totals
        , SUM(bp.missedshot_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS missedshots_away
        , SUM(bp.missedshot_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS missedshots_home
        -- Count blockedshot totals
        , SUM(bp.blockedshot_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS blockedshots_away
        , SUM(bp.blockedshot_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS blockedshots_home
        -- Count penalty totals
        , SUM(bp.penalty_away) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS penalties_away
        , SUM(bp.penalty_home) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) AS penalties_home
        -- Cumulative goal counts
        ,bp.goals_away
        ,bp.goals_home
        -- Cumulative goal descriptors (current state)
        ,ABS(bp.goals_home - bp.goals_away) as goal_difference_current
        ,CASE 
            WHEN(bp.goals_home - bp.goals_away) = 0 
                THEN 'TIE'
            WHEN(bp.goals_home - bp.goals_away) > 0 
                THEN 'HOME'
            WHEN(bp.goals_home - bp.goals_away) < 0 
                THEN 'AWAY'
            ELSE NULL
        END AS winning_team_current
        ,CASE 
            WHEN ABS(bp.goals_home - bp.goals_away) = 0  
                THEN 'TIE'
            WHEN ABS(bp.goals_home - bp.goals_away) = 1  
                THEN 'CLOSE'
            WHEN ABS(bp.goals_home - bp.goals_away) = 2  
                THEN 'BUFFER'
            WHEN ABS(bp.goals_home - bp.goals_away) = 3  
                THEN 'COMFORTABLE'
            WHEN ABS(bp.goals_home - bp.goals_away) >= 4 
                THEN 'BLOWOUT'
            ELSE NULL
        END AS game_state_current
        -- First goal flag
        ,CASE
            WHEN MIN(IF(bp.event_type = 'GOAL', bp.event_idx, NULL)) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = bp.event_idx
                THEN 1
            ELSE 0
        END AS first_goal_scored
        -- Last goal flag
        ,CASE
            WHEN MAX(IF(bp.event_type = 'GOAL', bp.event_idx, NULL)) OVER (PARTITION BY game_id ORDER BY  bp.game_id, event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = bp.event_idx
                THEN 1
            ELSE 0
        END AS last_goal_scored
        -- Cumulative goal descriptors (previous state)
        ,CASE 
            WHEN LAG(bp.event_idx, 1) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_home, 1) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 2) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_home, 2) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 3) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_home, 3) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 4) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_home, 4) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 5) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_home, 5) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
                ELSE NULL
            END as goals_home_lag
        ,CASE 
            WHEN LAG(bp.event_idx, 1) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_away, 1) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 2) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_away, 2) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 3) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_away, 3) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 4) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_away, 4) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
            WHEN LAG(bp.event_idx, 5) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)) <> bp.event_idx
                THEN LAG(bp.goals_away, 5) OVER (PARTITION BY game_id ORDER BY  bp.game_id,event_idx, (bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed))
                ELSE NULL
            END as goals_away_lag

    FROM 
        CTE_BASE_PLAYS bp

    ORDER BY 
        bp.game_id
        ,(bp.play_minutes_elapsed * 60)  + (bp.play_seconds_elapsed)
        ,event_idx
        )

-- CTE to determine the state of the game as a result of the play
, CTE_GAME_STATE as (
    SELECT 
        c.*
        -- Previous winning team
        ,ABS(c.goals_home_lag - c.goals_away_lag) as goal_difference_lag
        ,CASE
            WHEN(c.goals_home_lag - c.goals_away_lag) = 0 
                THEN 'TIE'
            WHEN(c.goals_home_lag - c.goals_away_lag) > 0 
                THEN 'HOME'
            WHEN(c.goals_home_lag - c.goals_away_lag) < 0 
                THEN 'AWAY'
            ELSE NULL
        END AS winning_team_lag
        -- Previous game state
        ,CASE 
            WHEN ABS(c.goals_home_lag - c.goals_away_lag) = 0  
                THEN 'TIE'
            WHEN ABS(c.goals_home_lag - c.goals_away_lag) = 1  
                THEN 'CLOSE'
            WHEN ABS(c.goals_home_lag - c.goals_away_lag) = 2  
                THEN 'BUFFER'
            WHEN ABS(c.goals_home_lag - c.goals_away_lag) = 3  
                THEN 'COMFORTABLE'
            WHEN ABS(c.goals_home_lag - c.goals_away_lag) >= 4 
                THEN 'BLOWOUT'
            ELSE NULL
        END AS game_state_lag
        -- Home - what was the result of the play on score?
        ,CASE 
            WHEN c.goal_difference_current = ABS(c.goals_home_lag - c.goals_away_lag)
                THEN 'NO CHANGE'
            WHEN c.goals_home > c.goals_home_lag
            AND c.goals_home < c.goals_away
                THEN 'CHASE GOAL'
            WHEN c.goals_home > c.goals_home_lag
            AND c.goals_home = c.goals_away
                THEN 'TYING GOAL SCORED'
            WHEN c.goals_home < c.goals_home_lag
            AND c.goals_home = c.goals_away
                THEN 'TYING GOAL ALLOWED'
            WHEN c.goals_home > c.goals_home_lag
            AND c.goals_home_lag = c.goals_away_lag
            AND c.goals_home > c.goals_away
                THEN 'GO-AHEAD GOAL SCORED'
            WHEN c.goals_home > c.goals_home_lag
            AND c.goals_home_lag > c.goals_away_lag
            AND c.goals_home > c.goals_away
                THEN 'BUFFER GOAL'
            WHEN c.goals_away > c.goals_away_lag
            AND c.goals_away_lag = c.goals_home_lag
            AND c.goals_away > c.goals_home
                THEN 'GO-AHEAD GOAL ALLOWED'
            ELSE NULL
        END AS home_result_of_play
        -- Away - what was the result of the play on score?
        ,CASE 
            WHEN c.goal_difference_current = ABS(c.goals_home_lag - c.goals_away_lag)
                THEN 'NO CHANGE'
            WHEN c.goals_away > c.goals_away_lag
            AND c.goals_away < c.goals_home
                THEN 'CHASE GOAL'
            WHEN c.goals_away > c.goals_away_lag
            AND c.goals_away = c.goals_home
                THEN 'TYING GOAL SCORED'
            WHEN c.goals_home > c.goals_home_lag
            AND c.goals_home = c.goals_away
                THEN 'TYING GOAL ALLOWED'
            WHEN c.goals_away > c.goals_away_lag
            AND c.goals_away_lag = c.goals_home_lag
            AND c.goals_away > c.goals_home
                THEN 'GO-AHEAD GOAL SCORED'
            WHEN c.goals_away > c.goals_away_lag
            AND c.goals_away_lag > c.goals_home_lag
            AND c.goals_away > c.goals_home
                THEN 'BUFFER GOAL'
            WHEN c.goals_home > c.goals_home_lag
            AND c.goals_home_lag = c.goals_away_lag
            AND c.goals_home > c.goals_away
                THEN 'GO-AHEAD GOAL ALLOWED'
            ELSE NULL
        END AS away_result_of_play
        -- Either team - last goal a game winning goal?
        ,CASE 
            WHEN c.last_goal_scored = 1                       -- last goal
            AND ABS(c.goals_away_lag - c.goals_home_lag) = 0  -- game was tied last play
            AND ABS(c.goals_away - c.goals_home) <> 0         -- game no longer tied
                THEN 1
            ELSE 0
        END AS last_goal_game_winning     
        -- Either team - last goal game tying?
        ,CASE 
            WHEN c.last_goal_scored = 1                       -- last goal
            AND ABS(c.goals_away_lag - c.goals_home_lag) = 1  -- game was within 1 goal last play
            AND ABS(c.goals_away - c.goals_home) = 0          -- game now tied
                THEN 1
            ELSE 0
        END AS last_goal_game_tying

    FROM
        CTE_CUMULATIVE as c

)

-- Final return
SELECT
    /* Primary Key */
    g.id

    /* Foreign Keys */
    ,g.game_id
    ,g.event_idx
    ,g.event_id
    ,g.player_id
    ,g.team_id

    /* Properties */
    ,g.player_role
    ,g.player_role_team 
    ,g.event_type
    ,g.event_code
    ,g.event_description
    ,g.play_x_coordinate
    ,g.play_y_coordinate
    ,g.play_period
    ,g.play_period_type
    ,g.play_period_time_elapsed
    ,g.play_period_time_remaining
    ,g.play_period_seconds_elapsed
    ,g.play_period_seconds_remaining
    ,g.play_total_seconds_elapsed
    ,g.play_total_seconds_remaining
    ,g.play_time
    ,g.shots_away
    ,g.shots_home
    ,g.hits_away
    ,g.hits_home
    ,g.faceoffs_away
    ,g.faceoffs_home
    ,g.takeaways_away
    ,g.takeaways_home
    ,g.giveaways_away
    ,g.giveaways_home
    ,g.missedshots_away
    ,g.missedshots_home
    ,g.blockedshots_away
    ,g.blockedshots_home
    ,g.penalties_away
    ,g.penalties_home
    ,g.first_goal_scored
    ,g.last_goal_scored
    ,g.goals_away
    ,g.goals_home
    ,g.goal_difference_current
    ,g.winning_team_current
    ,g.game_state_current
    ,g.home_result_of_play
    ,g.away_result_of_play
    ,g.last_goal_game_winning     
    ,g.last_goal_game_tying
    ,g.goals_home_lag
    ,g.goals_away_lag
    ,g.goal_difference_lag
    ,g.winning_team_lag
    ,g.game_state_lag

FROM
    CTE_GAME_STATE g