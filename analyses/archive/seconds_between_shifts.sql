with game_seconds as (
    select -1 + row_number() over() as seconds
    from unnest((select split(format("%10800s", ""), '') as h from (select null))) as pos
    order by seconds
)

, seconds_between_shifts as (
    select
        concat(shifts.shift_id, '_', gs.seconds) as new_shift_id
        , gs.seconds as game_time_seconds
        , (gs.seconds - shifts.start_seconds_elapsed) as shift_time_seconds
        , players.primary_position_abbreviation
        , case when gs.seconds = shifts.start_seconds_elapsed then true else false end as is_shift_start
        , case when gs.seconds = shifts.end_seconds_elapsed then true else false end as is_shift_end
        , shifts.*
    from `nhl-breakouts.dbt_dom.stg_nhl__shifts` as shifts
    inner join game_seconds as gs on gs.seconds between shifts.start_seconds_elapsed and shifts.end_seconds_elapsed
    left join `nhl-breakouts.dbt_dom.d_players` as players on players.player_id = shifts.player_id
    where 1 = 1
        and shifts.game_id = 2015021169
)

, home_player_lists as (
    select
        sbs.team_id
        , sbs.home_away_team
        , sbs.game_time_seconds
        , string_agg(cast(sbs.player_id as string)) as player_list
        , sum(case when sbs.primary_position_abbreviation = 'G' then 1 else 0 end) as goalie_on_ice
        , sum(case when sbs.primary_position_abbreviation = 'D' then 1 else 0 end) as defence_on_ice
        , sum(case when sbs.primary_position_abbreviation not in ('G', 'D') then 1 else 0 end) as forward_on_ice
    from seconds_between_shifts as sbs
    where 1 = 1
        and sbs.home_away_team = 'home'
        and sbs.is_shift_start is false -- do not include those who just started their shift
    group by
        sbs.team_id
        , sbs.home_away_team
        , sbs.game_time_seconds
)

, away_player_lists as (
    select
        sbs.team_id
        , sbs.home_away_team
        , sbs.game_time_seconds
        , string_agg(cast(sbs.player_id as string)) as player_list
        , sum(case when sbs.primary_position_abbreviation = 'G' then 1 else 0 end) as goalie_on_ice
        , sum(case when sbs.primary_position_abbreviation = 'D' then 1 else 0 end) as defence_on_ice
        , sum(case when sbs.primary_position_abbreviation not in ('G', 'D') then 1 else 0 end) as forward_on_ice
    from seconds_between_shifts as sbs
    where 1 = 1
        and sbs.home_away_team = 'away'
        and sbs.is_shift_start is false -- do not include those who just started their shift
    group by
        sbs.team_id
        , sbs.home_away_team
        , sbs.game_time_seconds
)

select
    sbs.new_shift_id
    , sbs.shift_number
    , sbs.game_id
    , sbs.player_id
    , sbs.team_id
    , sbs.home_away_team
    , sbs.game_type_description
    , sbs.event_number
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
    , sbs.start_seconds_elapsed
    , sbs.end_seconds_elapsed
    , sbs.duration_seconds_elapsed
    , sbs.start_time
    , sbs.end_time
    , sbs.duration
    , concat((hpl.defence_on_ice + hpl.forward_on_ice), 'v', (apl.defence_on_ice + apl.forward_on_ice)) as game_state
    , concat('home:', (hpl.defence_on_ice + hpl.forward_on_ice), '-away:', (apl.defence_on_ice + apl.forward_on_ice)) as game_state_description
    , case
        when (hpl.defence_on_ice + hpl.forward_on_ice) = (apl.defence_on_ice + apl.forward_on_ice) then 'even strength'
        when sbs.home_away_team = 'home' and (hpl.defence_on_ice + hpl.forward_on_ice) > (apl.defence_on_ice + apl.forward_on_ice) then 'skater advantage'
        when sbs.home_away_team = 'home' and (hpl.defence_on_ice + hpl.forward_on_ice) < (apl.defence_on_ice + apl.forward_on_ice) then 'skater disadvantage'
        when sbs.home_away_team = 'away' and (apl.defence_on_ice + apl.forward_on_ice) > (hpl.defence_on_ice + hpl.forward_on_ice) then 'skater advantage'
        when sbs.home_away_team = 'away' and (apl.defence_on_ice + apl.forward_on_ice) < (hpl.defence_on_ice + hpl.forward_on_ice) then 'skater disadvantage'
        else 'unknown'
        end as game_state_skaters
    , case when hpl.goalie_on_ice = 0 then true else false end as home_goalie_pulled
    , case when apl.goalie_on_ice = 0 then true else false end as away_goalie_pulled
    , hpl.player_list as home_player_list
    , apl.player_list as away_player_list
    , (hpl.defence_on_ice + hpl.forward_on_ice) as home_skaters_on_ice
    , (apl.defence_on_ice + apl.forward_on_ice) as away_skaters_on_ice
    , hpl.goalie_on_ice as home_goalie_on_ice
    , hpl.defence_on_ice as home_defence_on_ice
    , hpl.forward_on_ice as home_forward_on_ice
    , apl.goalie_on_ice as away_goalie_on_ice
    , apl.defence_on_ice as away_defence_on_ice
    , apl.forward_on_ice as away_forward_on_ice
from seconds_between_shifts as sbs
left join home_player_lists as hpl on hpl.game_time_seconds = sbs.game_time_seconds
left join away_player_lists as apl on apl.game_time_seconds = sbs.game_time_seconds

