select
    /* Primary Key */
    stg_meltano__schedule.id

    /* Foreign Keys */
    ,stg_meltano__schedule.game_id
    ,stg_meltano__schedule.season_id
    ,stg_meltano__schedule.away_team_id
    ,stg_meltano__schedule.home_team_id
    ,stg_meltano__schedule.venue_id

    /* Properties */
    ,stg_meltano__schedule.game_number
    ,stg_meltano__schedule.url
    ,stg_meltano__schedule.game_type
    ,stg_meltano__schedule.game_date
    ,stg_meltano__schedule.abstract_game_state
    ,stg_meltano__schedule.coded_game_state
    ,stg_meltano__schedule.detailed_state
    ,stg_meltano__schedule.status_code
    ,stg_meltano__schedule.is_start_time_tbd
    ,stg_meltano__schedule.away_team_wins
    ,stg_meltano__schedule.away_team_losses
    ,stg_meltano__schedule.away_team_ot
    ,stg_meltano__schedule.away_team_type
    ,stg_meltano__schedule.away_team_score
    ,stg_meltano__schedule.away_team_name
    ,stg_meltano__schedule.away_team_url
    ,stg_meltano__schedule.home_team_wins
    ,stg_meltano__schedule.home_team_losses
    ,stg_meltano__schedule.home_team_ot
    ,stg_meltano__schedule.home_team_type
    ,stg_meltano__schedule.home_team_score
    ,stg_meltano__schedule.home_team_name
    ,stg_meltano__schedule.home_team_url
    ,stg_meltano__schedule.venue_name
    ,stg_meltano__schedule.venue_url
    ,stg_meltano__schedule.content_url
    ,stg_meltano__schedule.extracted_at
    ,stg_meltano__schedule.loaded_at

from {{ ref('stg_meltano__schedule') }}
