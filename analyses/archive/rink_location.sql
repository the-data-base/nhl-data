-- #cte: depending on which end the team is shooting on, adjust the x and y play coordinates so that all plays are going towards the same end (all shots towards right end)
with adj_coordinates as (
  select
    plays.play_id
    , plays.event_type
    , plays.play_x_coordinate
    , plays.play_y_coordinate
    , lower(plays.player_role_team) as player_role_team
    , plays.play_period
    , schedule.home_period1_shooting
    -- Flip the x-y coords based on where the player's team was shooting in the first period of the game (reminder that when mod(x, 2) = 0 then odd number, but when > 0 then  even number)
      , case
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64)
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64)
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64)
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64)
        end as adj_play_x_coordinate
      , case
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64)
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64)
          when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64)
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64)*-1
          when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64)
        end as adj_play_y_coordinate
  from `nhl-breakouts.dbt_dom.f_plays` as plays
  left join `nhl-breakouts.dbt_dom.d_schedule` as schedule on schedule.game_id = plays.game_id
)

select
  ac.*
-- Rink distance from goal (shooting)
  , sqrt(power((89 - abs(adj_play_x_coordinate)),2) + power(adj_play_y_coordinate,2)) as goal_distance
-- Rink angle from goal (shooting)
  , atan( (adj_play_y_coordinate / (89 - adj_play_x_coordinate)) ) * (180 / (acos(-1))) as angle
-- Rink zones
  , case
    when adj_play_x_coordinate between -25 and 25 then 'neutral_zone'
    when adj_play_x_coordinate between -100 and -25 then 'defensive_zone'
    else 'offensive_zone'
    end as zone_type
  , case
    when adj_play_x_coordinate between -25 and 25 then 'neutral_zone'
    when adj_play_x_coordinate between -100 and -25 then 'defensive_zone'
    when (adj_play_x_coordinate between 25 and 54) and (adj_play_y_coordinate between -42.5 and -7) then 'r_point'
    when (adj_play_x_coordinate between 25 and 54) and (adj_play_y_coordinate between -7 and 7) then 'c_point'
    when (adj_play_x_coordinate between 25 and 54) and (adj_play_y_coordinate between 7 and 42.5) then 'l_point'
    when (adj_play_x_coordinate between 54 and 69) and (adj_play_y_coordinate between -42.5 and -22) then 'r_1_high'
    when (adj_play_x_coordinate between 54 and 69) and (adj_play_y_coordinate between -22 and -7) then 'r_2_high'
    when (adj_play_x_coordinate between 52 and 69) and (adj_play_y_coordinate between -7 and 7) then 'high_slot'
    when (adj_play_x_coordinate between 52 and 69) and (adj_play_y_coordinate between 7 and 22) then 'l_2_high'
    when (adj_play_x_coordinate between 52 and 69) and (adj_play_y_coordinate between 22 and 42.5) then 'l_1_high'
    when (adj_play_x_coordinate between 69 and 89) and (adj_play_y_coordinate between -42.5 and -22) then 'r_1_low'
    when (adj_play_x_coordinate between 69 and 89) and (adj_play_y_coordinate between -22 and -7) then 'r_2_low'
    when (adj_play_x_coordinate between 69 and 89) and (adj_play_y_coordinate between -7 and 7) then 'low_slot'
    when (adj_play_x_coordinate between 69 and 89) and (adj_play_y_coordinate between 7 and 22) then 'l_2_low'
    when (adj_play_x_coordinate between 69 and 89) and (adj_play_y_coordinate between 22 and 42.5) then 'l_1_low'
    when (adj_play_x_coordinate between 89 and 100) then 'down_low'
    end as zone
from adj_coordinates as ac
limit 1000

