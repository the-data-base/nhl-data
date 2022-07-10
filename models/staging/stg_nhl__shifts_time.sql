with

game_seconds as (
    -- generates a series of 10800 seconds
    select -1 + row_number() over() as seconds
    from unnest((select split(format("%10800s", ""), '') as h from (select null))) as pos -- noqa: disable=L036,L042
    order by seconds -- noqa: enable=L036,L042
)

, player_shift_seconds as (
    select
        concat(shifts.new_shift_id, "_", gs.seconds) as new_shift_id
        , shifts.new_shift_number
        , shifts.game_id
        , shifts.player_id
        , shifts.team_id
        , shifts.home_away_team
        , shifts.shift_ids
        , shifts.shift_numbers
        , shifts.event_numbers
        , shifts.game_type_description
        , shifts.type_code
        , shifts.detail_code
        , shifts.player_full_name
        , shifts.is_goal
        , shifts.is_period_start
        , shifts.is_period_end
        , shifts.goal_game_state
        , shifts.goal_assisters
        , shifts.goal_primary_assister_full_name
        , shifts.goal_secondary_assister_full_name
        , shifts.period
        , shifts.period_type
        , shifts.start_seconds_elapsed
        , shifts.end_seconds_elapsed
        , shifts.duration_seconds_elapsed
        , shifts.start_time
        , shifts.end_time
        , shifts.duration
        , gs.seconds as game_time_seconds
        , (gs.seconds - shifts.start_seconds_elapsed) as shift_time_seconds
        , players.primary_position_abbreviation
        , case when gs.seconds = shifts.start_seconds_elapsed then true else false end as is_shift_start
        , case when gs.seconds = shifts.end_seconds_elapsed then true else false end as is_shift_end
        , case when gs.seconds = shifts.start_seconds_elapsed and shifts.is_period_start is true then true else false end as is_shift_start_period_start
        , case when gs.seconds = shifts.start_seconds_elapsed and shifts.is_period_start is false then true else false end as is_shift_start_not_period_start
        , case when gs.seconds = shifts.start_seconds_elapsed and shifts.is_period_end is true then true else false end as is_shift_end_period_end

    from {{ ref('stg_nhl__shifts') }} as shifts
    inner join game_seconds as gs on gs.seconds between shifts.start_seconds_elapsed and shifts.end_seconds_elapsed
    left join {{ ref('d_players') }} as players on players.player_id = shifts.player_id
    --where (case when gs.seconds = shifts.start_seconds_elapsed and is_period_end is true then true else false end) is false -- remove the shift that ends the period
    {# where shifts.game_id = 2015021169 #}
)

, game_second_skaters_on_ice as (
    -- lists players and count of positions on ice for each game second
    select
        game_id
        , period
        , game_time_seconds
        , sum(case when home_away_team = 'home' and primary_position_abbreviation = 'G' and is_shift_start_not_period_start is false then 1 else 0 end) as home_goalie_on_ice
        , sum(case when home_away_team = 'away' and primary_position_abbreviation = 'G' and is_shift_start_not_period_start is false then 1 else 0 end) as away_goalie_on_ice
        , sum(case when home_away_team = 'home' and primary_position_abbreviation = 'D' and is_shift_start_not_period_start is false then 1 else 0 end) as home_defence_on_ice
        , sum(case when home_away_team = 'away' and primary_position_abbreviation = 'D' and is_shift_start_not_period_start is false then 1 else 0 end) as away_defence_on_ice
        , sum(case when home_away_team = 'home' and primary_position_abbreviation not in ('G', 'D') and is_shift_start_not_period_start is false then 1 else 0 end) as home_forward_on_ice
        , sum(case when home_away_team = 'away' and primary_position_abbreviation not in ('G', 'D') and is_shift_start_not_period_start is false then 1 else 0 end) as away_forward_on_ice
        , array_agg(case when home_away_team = 'home' and is_shift_start_not_period_start is false then player_id end ignore nulls) as home_skaters
        , array_agg(case when home_away_team = 'away' and is_shift_start_not_period_start is false then player_id end ignore nulls) as away_skaters
    from player_shift_seconds
    group by 1, 2, 3
)

-- as of 7/10/2022, 14k duplicates were introduced b/c of shifts that start and end back-to-back (yep, shifts data is brutal)
-- rule: if a player's shift overlaps, choose the shift that started earlier as the one to keep (min takes the first shift, max takes the susbsequent)
, dedup_game_time_seconds as (
    select
        pss.game_id
        , pss.player_id
        , pss.game_time_seconds
        , pss.period
        , min(new_shift_id) as keep_new_shift_id
        , max(new_shift_id) as remove_new_shift_id
        , count(*) as test
        , string_agg(new_shift_id) as all_new_shift_ids
    from player_shift_seconds as pss
    where pss.is_goal is not true
    group by 1, 2, 3, 4
    having count(*) > 1
    order by 1, 2, 3, 4
)

select
    sbs.new_shift_id
    , sbs.new_shift_number
    , sbs.game_id
    , sbs.player_id
    , sbs.team_id
    , sbs.home_away_team
    , sbs.game_type_description
    , sbs.shift_ids
    , sbs.shift_numbers
    , sbs.event_numbers
    , sbs.type_code
    , sbs.detail_code
    , sbs.player_full_name
    , sbs.is_goal
    , sbs.goal_game_state
    , sbs.goal_assisters
    , sbs.goal_primary_assister_full_name
    , sbs.goal_secondary_assister_full_name
    , sbs.period
    , sbs.period_type
    , sbs.game_time_seconds
    , sbs.shift_time_seconds
    , sbs.is_shift_start
    , sbs.is_shift_end
    , sbs.is_shift_start_period_start
    , sbs.is_shift_start_not_period_start
    , sbs.is_shift_end_period_end
    , sbs.start_seconds_elapsed
    , sbs.end_seconds_elapsed
    , sbs.duration_seconds_elapsed
    , sbs.start_time
    , sbs.end_time
    , sbs.duration
    , sbs.is_period_start
    , sbs.is_period_end
    , concat((soi.home_defence_on_ice + soi.home_forward_on_ice), 'v', (soi.away_defence_on_ice + soi.away_forward_on_ice)) as game_state
    , concat('home:', (soi.home_defence_on_ice + soi.home_forward_on_ice), '-away:', (soi.away_defence_on_ice + soi.away_forward_on_ice)) as game_state_description
    , case
        when (soi.home_defence_on_ice + soi.home_forward_on_ice) = (soi.away_defence_on_ice + soi.away_forward_on_ice) then 'even strength'
        when sbs.home_away_team = 'home' and (soi.home_defence_on_ice + soi.home_forward_on_ice) > (soi.away_defence_on_ice + soi.away_forward_on_ice) then 'skater advantage'
        when sbs.home_away_team = 'home' and (soi.home_defence_on_ice + soi.home_forward_on_ice) < (soi.away_defence_on_ice + soi.away_forward_on_ice) then 'skater disadvantage'
        when sbs.home_away_team = 'away' and (soi.away_defence_on_ice + soi.away_forward_on_ice) > (soi.home_defence_on_ice + soi.home_forward_on_ice) then 'skater advantage'
        when sbs.home_away_team = 'away' and (soi.away_defence_on_ice + soi.away_forward_on_ice) < (soi.home_defence_on_ice + soi.home_forward_on_ice) then 'skater disadvantage'
        else 'unknown'
    end as game_state_skaters
    , soi.home_goalie_on_ice = 0 as home_goalie_pulled
    , soi.away_goalie_on_ice = 0 as away_goalie_pulled
    , soi.home_skaters
    , soi.away_skaters
    , (soi.home_defence_on_ice + soi.home_forward_on_ice) as home_skaters_on_ice
    , (soi.away_defence_on_ice + soi.away_forward_on_ice) as away_skaters_on_ice
    , soi.home_goalie_on_ice
    , soi.home_defence_on_ice
    , soi.home_forward_on_ice
    , soi.away_goalie_on_ice
    , soi.away_defence_on_ice
    , soi.away_forward_on_ice
from player_shift_seconds as sbs
left join game_second_skaters_on_ice as soi
    on sbs.game_id = soi.game_id
        and sbs.game_time_seconds = soi.game_time_seconds
        and sbs.period = soi.period
left join dedup_game_time_seconds as d on sbs.new_shift_id = d.remove_new_shift_id
where d.remove_new_shift_id is null
