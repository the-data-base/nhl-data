
-- Goal: create a mapping table that we can join to with game_id & period & team from play-by-play data to know how to interpret the x-y coords
-- Method: based on the assumptions that the majority of shots will come close to the net, classify the area of the ice with the most shots as the "shooting end", to then make axes adjustments to

-- #cte1: pull all home team shots in the the first and third periods
with p1p3_plays_shots_home as (
    select
        plays.stg_nhl__live_plays_id as play_id
        , plays.game_id
        , schedule.game_type
        , schedule.game_type_description
        , plays.play_period
        , plays.team_id
        , plays.event_id
        , plays.event_type
        , teams.full_name as team_full_name
        , plays.event_description
        , plays.play_x_coordinate
        , plays.play_y_coordinate
    from
        {{ ref('stg_nhl__live_plays') }} as plays
    inner join {{ ref('stg_nhl__teams') }} as teams on teams.team_id = plays.team_id
    inner join {{ ref('stg_nhl__schedule') }} as schedule on schedule.game_id = plays.game_id
    where 1 = 1
        and (plays.play_period = 1 or plays.play_period = 3)
        and lower(plays.event_type) in ('goal', 'missed_shot', 'shot')
        and lower(plays.player_role_team) = 'home'
        and lower(plays.player_role) in ('shooter', 'scorer')
        and schedule.home_team_id = plays.team_id
    order by
        plays.game_id desc
)

-- #cte2: summarize shot at the game & team level for period 1
, p1_team_game_shots_home as (
    select
        ps.game_id
        , ps.game_type
        , ps.game_type_description
        , ps.team_id
        , ps.team_full_name
        , ps.play_period
        , count(1) as p1_shots
        , sum(case when cast(ps.play_x_coordinate as float64) > 0 then 1 else 0 end) as p1_shots_right
        , sum(case when cast(ps.play_x_coordinate as float64) < 0 then 1 else 0 end) as p1_shots_left
    from
        p1p3_plays_shots_home as ps
    where 1 = 1
        and play_period = 1
    group by 1, 2, 3, 4, 5, 6
)

-- #cte3: summarize shot at the game & team level for period 1
, p3_team_game_shots_home as (
    select
        ps.game_id
        , ps.game_type
        , ps.game_type_description
        , ps.team_id
        , ps.team_full_name
        , ps.play_period
        , count(1) as p3_shots
        , sum(case when cast(ps.play_x_coordinate as float64) > 0 then 1 else 0 end) as p3_shots_right
        , sum(case when cast(ps.play_x_coordinate as float64) < 0 then 1 else 0 end) as p3_shots_left
    from
        p1p3_plays_shots_home as ps
    where 1 = 1
        and play_period = 3
    group by 1, 2, 3, 4, 5, 6
)

--#return: period 1 shooting location for the home team by game_id
select
    p1.game_id
    , p1.team_id
    , p1.game_type_description
    , p1.team_full_name
    , p1.p1_shots
    , p1.p1_shots_left
    , p1.p1_shots_right
    , p3.p3_shots
    , p3.p3_shots_left
    , p3.p3_shots_right
    -- in case period 1 is not enough to determine a shooting side, then bring in period 3 as well
    , case
        when (p1.p1_shots_right) + (p1.p1_shots_left) = 0 then 'missing'
        when p1.p1_shots_right > p1.p1_shots_left then 'right'
        when p1.p1_shots_right < p1.p1_shots_left then 'left'
        when (p1.p1_shots_right + p3.p3_shots_right) > (p1.p1_shots_left + p3.p3_shots_left) then 'right'
        when (p1.p1_shots_right + p3.p3_shots_right) < (p1.p1_shots_left + p3.p3_shots_left) then 'left'
    end as p1_shooting_location
from
    p1_team_game_shots_home as p1
left join p3_team_game_shots_home as p3 on p3.game_id = p1.game_id and p3.team_id = p1.team_id
order by p1.game_id
