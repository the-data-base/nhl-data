with

deduped as (
  select *
  from {{ source('xg', 'xg_*') }}
  qualify row_number() over (
    partition by
      id_play_id
      , id_player_id
  ) = 1
)

select
  /* Primary Key */
  {{ dbt_utils.surrogate_key(['deduped.id_play_id']) }} as stg_nhl__xg_id

  /* Identifiers */
  , concat(id_model_type, "_target_", id_season_id, "_training_", id_model_season_range) as xg_model_id
  , id_play_id
  , id_game_id
  , id_game_type
  , id_season_id
  , id_season_year
  , id_player_id
  , id_player_full_name
  , id_strength_state
  , id_strength_state_code
  , id_is_home
  , id_fenwick_shot

  /* Actual vs Predictions */
  , x_goal
  , xg_proba
  , xg_pred

  /* Model results */
  , id_model_type as model_type
  , id_model_rocauc as model_rocauc
  , id_model_prauc as model_prauc
  , id_model_logloss as model_logloss
  , id_model_precision as model_precision
  , id_model_recall as model_recall
  , id_model_seasons as model_seasons
  , id_model_season_range as model_season_range
  , id_model_fenwick_shots as model_fenwick_shots
  , id_model_pcnt_goals as model_pcnt_goals

  /* Model features (Properties) */
  , shot_distance
  , shot_angle
  , shot_xcoord
  , shot_ycoord
  , shot_backhand
  , shot_deflected
  , shot_slap
  , shot_snap
  , shot_tip
  , shot_wrap
  , shot_wrist
  , shot_rebound
  , game_seconds_elapsed
  , period_seconds_elapsed
  , period
  , is_home
  , game_state_5v5
  , game_state_4v4
  , game_state_3v3
  , seconds_since_last_shot
  , id_time_loaded as _time_loaded
  , id_model_insert_data_ts as _data_time_loaded
  , id_model_insert_model_ts as _model_time_loaded

from deduped
