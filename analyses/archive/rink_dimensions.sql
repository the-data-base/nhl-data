-- CTE to pull all home team shots in the first period from non-goalie skaters
WITH home_shot_data AS (
  SELECT
    p.play_id
    , DATE(p.play_time) AS game_date
    , p.game_id
    , p.event_id
    , p.event_type
    , p.team_id
    , t.full_name AS team_full_name
    , p.player_role_team
    , p.player_id
    , p.player_full_name
    , p.event_code
    , p.event_description
    , p.play_x_coordinate
    , p.play_y_coordinate
    , p.play_period
    , p.play_period_time_elapsed

  FROM
    `nhl-breakouts.analytics_intermediate.f_plays` p
    INNER JOIN `nhl-breakouts.analytics_intermediate.d_teams` t ON p.team_id = t.team_id
    INNER JOIN `nhl-breakouts.analytics_intermediate.d_players`pl ON p.player_id = pl.player_id

  WHERE 1=1
    AND p.event_type IN ('GOAL', 'MISSED_SHOT', 'SHOT')
    AND p.play_period = 1
    AND p.player_role_team = 'HOME'
    AND pl.primary_position_name <> 'Goalie'

  ORDER BY
    DATE(p.play_time) DESC
)

-- Determine which end the home team is shooting at during the first period for each team
, first_period_shot_location AS (
  SELECT
    sd.team_id
    , sd.team_full_name
    , SUM(CASE WHEN CAST(sd.play_x_coordinate AS FLOAT64) > 0 THEN 1 ELSE 0 END) AS right_end
    , SUM(CASE WHEN CAST(sd.play_x_coordinate AS FLOAT64) < 0 THEN 1 ELSE 0 END) AS left_end

  FROM
    home_shot_data sd

  WHERE 1=1

  GROUP BY
    sd.team_id
    , sd.team_full_name
)

-- Depending on which end a majority of shots are towards, assign each team a home_first_period_shooting_end
, shooting_end AS (
  SELECT
    DISTINCT sd.team_id
    , CASE WHEN fp.right_end > fp.left_end THEN 'right_end' ELSE 'left_end' END AS home_first_period_shooting_end

  FROM
    home_shot_data sd
    INNER JOIN first_period_shot_location fp ON sd.team_id = fp.team_id
)

-- Depending on which end the team is shooting on, adjust the x and y play coordinates so that all shots are taken towards the same end (all shots towards right end)
, adjusted_coordinates AS (
  SELECT
    p.play_id
    , DATE(p.play_time) AS game_date
    , p.game_id
    , p.event_id
    , p.event_type
    , p.team_id
    , t.full_name AS team_full_name
    , p.player_role_team
    , p.player_id
    , p.player_full_name
    , p.event_code
    , p.event_description
    , p.play_x_coordinate
    , p.play_y_coordinate
    , CASE
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_x_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_x_coordinate AS FLOAT64)
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_x_coordinate AS FLOAT64)
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_x_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_x_coordinate AS FLOAT64)
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_x_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_x_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_x_coordinate AS FLOAT64)
      END AS adj_play_x_coordinate
    , CASE
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_y_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_y_coordinate AS FLOAT64)
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_y_coordinate AS FLOAT64)
        WHEN p.player_role_team = 'HOME' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_y_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_y_coordinate AS FLOAT64)
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'left_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_y_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) > 0 THEN CAST(p.play_y_coordinate AS FLOAT64)*-1
        WHEN p.player_role_team = 'AWAY' AND se.home_first_period_shooting_end = 'right_end' AND MOD(p.play_period, 2) = 0 THEN CAST(p.play_y_coordinate AS FLOAT64)
      END AS adj_play_y_coordinate
    , p.play_period
    , p.play_period_time_elapsed
    , g.home_team_id
    , g.away_team_id
    , se.home_first_period_shooting_end

  FROM
    `nhl-breakouts.analytics_intermediate.f_plays` p
    INNER JOIN `nhl-breakouts.analytics_intermediate.d_teams` t ON p.team_id = t.team_id
    INNER JOIN `nhl-breakouts.analytics_intermediate.d_players` pl ON p.player_id = pl.player_id
    INNER JOIN `nhl-breakouts.analytics_intermediate.f_games` g ON p.game_id = g.game_id
    INNER JOIN shooting_end se ON g.home_team_id = se.team_id

  WHERE 1=1
    AND p.event_type IN ('GOAL', 'MISSED_SHOT', 'SHOT')
)

-- Assign each shot a zone based on the adjusted coordinates
, rink_dimensions AS (
  SELECT
    ac.*
    , CASE
      WHEN adj_play_x_coordinate BETWEEN -25 AND 25 THEN 'NEUTRAL_ZONE'
      WHEN adj_play_x_coordinate BETWEEN -100 AND -25 THEN 'DEFENSIVE_ZONE'
      WHEN (adj_play_x_coordinate BETWEEN 25 AND 54) AND (adj_play_y_coordinate BETWEEN -42.5 AND -7) THEN 'R_POINT'
      WHEN (adj_play_x_coordinate BETWEEN 25 AND 54) AND (adj_play_y_coordinate BETWEEN -7 AND 7) THEN 'C_POINT'
      WHEN (adj_play_x_coordinate BETWEEN 25 AND 54) AND (adj_play_y_coordinate BETWEEN 7 AND 42.5) THEN 'L_POINT'
      WHEN (adj_play_x_coordinate BETWEEN 54 AND 69) AND (adj_play_y_coordinate BETWEEN -42.5 AND -22) THEN 'R_1_HIGH'
      WHEN (adj_play_x_coordinate BETWEEN 54 AND 69) AND (adj_play_y_coordinate BETWEEN -22 AND -7) THEN 'R_2_HIGH'
      WHEN (adj_play_x_coordinate BETWEEN 52 AND 69) AND (adj_play_y_coordinate BETWEEN -7 AND 7) THEN 'HIGH_SLOT'
      WHEN (adj_play_x_coordinate BETWEEN 52 AND 69) AND (adj_play_y_coordinate BETWEEN 7 AND 22) THEN 'L_2_HIGH'
      WHEN (adj_play_x_coordinate BETWEEN 52 AND 69) AND (adj_play_y_coordinate BETWEEN 22 AND 42.5) THEN 'L_1_HIGH'
      WHEN (adj_play_x_coordinate BETWEEN 69 AND 89) AND (adj_play_y_coordinate BETWEEN -42.5 AND -22) THEN 'R_1_LOW'
      WHEN (adj_play_x_coordinate BETWEEN 69 AND 89) AND (adj_play_y_coordinate BETWEEN -22 AND -7) THEN 'R_2_LOW'
      WHEN (adj_play_x_coordinate BETWEEN 69 AND 89) AND (adj_play_y_coordinate BETWEEN -7 AND 7) THEN 'LOW_SLOT'
      WHEN (adj_play_x_coordinate BETWEEN 69 AND 89) AND (adj_play_y_coordinate BETWEEN 7 AND 22) THEN 'L_2_LOW'
      WHEN (adj_play_x_coordinate BETWEEN 69 AND 89) AND (adj_play_y_coordinate BETWEEN 22 AND 42.5) THEN 'L_1_LOW'
      WHEN (adj_play_x_coordinate BETWEEN 89 AND 100) THEN 'DOWN_LOW'
    END AS zone
  FROM
    adjusted_coordinates ac
)

-- QC results
SELECT *
FROM rink_dimensions
LIMIT 5;
