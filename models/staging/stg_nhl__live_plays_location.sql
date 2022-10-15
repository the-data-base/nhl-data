-- #cte: depending on which end the team is shooting on, adjust the x and y play coordinates so that all plays are going towards the same end (all shots towards right end)
with adj_coordinates as (
    select
        plays.stg_nhl__live_plays_id as play_id
        , plays.game_id
        , plays.event_id
        , plays.player_id
        , plays.team_id
        , plays.event_idx
        , schedule.game_type
        , plays.play_period
        , plays.event_type
        , lower(plays.player_role_team) as player_role_team
        , schedule.home_period1_shooting
        , cast(plays.play_x_coordinate as float64) as play_x_coordinate
        , cast(plays.play_y_coordinate as float64) as play_y_coordinate
        -- Flip the x-y coords based on where the player's team was shooting in the first period of the game (reminder that when mod(x, 2) = 0 then odd number, but when > 0 then  even number)
        , case
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64)
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64)
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64)
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_x_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_x_coordinate as float64)
        end as adj_play_x_coordinate
        , case
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64)
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64)
            when lower(plays.player_role_team) = 'home' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64)
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'left' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) > 0 then cast(plays.play_y_coordinate as float64) * -1
            when lower(plays.player_role_team) = 'away' and schedule.home_period1_shooting = 'right' and mod(plays.play_period, 2) = 0 then cast(plays.play_y_coordinate as float64)
        end as adj_play_y_coordinate
    from {{ ref('stg_nhl__live_plays') }} as plays
    left join {{ ref('d_schedule') }} as schedule on schedule.game_id = plays.game_id

)

select
    /* Primary Key */
    play_id
    /* Identifiers */
    , game_id
    , event_id
    , player_id
    , team_id
    , game_type
    , event_idx
    /* Properties */
    , play_period
    , event_type
    , player_role_team
    , home_period1_shooting
    , play_x_coordinate
    , play_y_coordinate
    , adj_play_x_coordinate
    , adj_play_y_coordinate
    -- Rink distance from goal (shooting)... hypotenuse = sqrt( (x2-x1)^2 + (y2-y1)^2 )... where y1 = 0 and x1 = 89
    , round(sqrt(power((89 - adj_play_x_coordinate), 2) + power(0 - adj_play_y_coordinate, 2)), 2) as play_distance
    -- Rink angle from goal (shooting)... angle = tan^-1( (y2-y1) / (x2-x1) )... where y1 = 0 and x1 = 89
    , case
        when adj_play_x_coordinate = 89 then 0
        else round(atan( ((adj_play_y_coordinate) / (89 - adj_play_x_coordinate)) ) * (180 / (acos(-1))), 2)
    end as play_angle
    -- Rink zones
    , case
        when adj_play_y_coordinate < 0 then 'left'
        when adj_play_y_coordinate > 0 then 'right'
        when adj_play_y_coordinate = 0 then 'center'
        else 'missing'
    end as rink_side
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

from adj_coordinates
