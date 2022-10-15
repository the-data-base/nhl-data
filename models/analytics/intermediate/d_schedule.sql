select
    /* Primary Key */
    schedule.stg_nhl__schedule_id as schedule_id

    /* Identifiers */
    , schedule.game_id
    , schedule.season_id
    , schedule.away_team_id
    , schedule.home_team_id
    , schedule.venue_id

    /* Schedule Properties */
    , schedule.game_number
    , schedule.game_type
    , schedule.game_type_description
    , schedule.game_date
    , schedule.abstract_game_state
    , schedule.coded_game_state
    , schedule.detailed_state
    , schedule.status_code
    , schedule.is_start_time_tbd
    , schedule.away_team_wins
    , schedule.away_team_losses
    , schedule.away_team_ot
    , schedule.away_team_type
    , schedule.away_team_score
    , schedule.away_team_name
    , schedule.home_team_wins
    , schedule.home_team_losses
    , schedule.home_team_ot
    , schedule.home_team_type
    , schedule.home_team_score
    , schedule.home_team_name
    , schedule.venue_name

    /* Rink Shooting Properties */
    -- ...this dataset brings in p1_shooting_location, which is the location where shots were going in period 1 for the home team

    -- ... now, classify for the home team with the assumption that each period switches sides
    , rs.p1_shooting_location as home_period1_shooting
    , case
        when rs.p1_shooting_location = 'right' then 'left'
        when rs.p1_shooting_location = 'left' then 'right'
        else rs.p1_shooting_location
    end as home_period2_shooting
    , rs.p1_shooting_location as home_period3_shooting
    -- ... now, classify for the away team with the assumption that each period switches sides
    , case
        when rs.p1_shooting_location = 'right' then 'left'
        when rs.p1_shooting_location = 'left' then 'right'
        else rs.p1_shooting_location
    end as away_period1_shooting
    , rs.p1_shooting_location as away_period2_shooting
    , case
        when rs.p1_shooting_location = 'right' then 'left'
        when rs.p1_shooting_location = 'left' then 'right'
        else rs.p1_shooting_location
    end as away_period3_shooting

    /* Additioanl properties */
    , schedule.home_team_url
    , schedule.away_team_url
    , schedule.venue_url
    , schedule.content_url
    , schedule.url
    , schedule.extracted_at
    , schedule.loaded_at
from
    {{ ref('stg_nhl__schedule') }} as schedule
left join {{ ref('stg_nhl__rink_shooting') }} as rs on rs.game_id = schedule.game_id

