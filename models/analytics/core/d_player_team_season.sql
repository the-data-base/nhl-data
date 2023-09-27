with
player_team_season as (
    select
        box.player_id
        , team.team_id
        , season.season_id
        , count(1) over (partition by box.player_id, season.season_id) as player_season_teams
        , count(distinct box.game_id) as games_played
        , sum(cast(split(box.time_on_ice, ':')[offset(0)] as int)) + (sum(cast(split(box.time_on_ice, ':')[offset(1)] as int)) / 60) as time_on_ice_minutes
        , min(schedule.game_date) as season_start_dt
        , max(schedule.game_date) as season_end_dt
        , max(season.season_id) over () as current_season_id
    from {{ ref('f_boxscore_player') }} as box
    left join {{ ref('d_schedule') }} as schedule on box.game_id = schedule.game_id
    left join{{ ref('d_seasons') }} as season on season.season_id = schedule.season_id
    left join {{ ref('d_teams') }} as team on box.team_id = team.team_id
    left join {{ ref('f_games_scratches') }} as scratches on box.player_id = scratches.player_id and box.game_id = scratches.game_id
    where schedule.game_type in ('02', '03') and scratches.player_id is null
    group by
        box.player_id
        , team.team_id
        , season.season_id
)

select
    /* primary key */
    {{ dbt_utils.surrogate_key(['pts.player_id', 'pts.team_id', 'pts.season_id']) }} as player_team_season_id
    /* foreign keys */
    , pts.player_id
    , pts.team_id
    , pts.season_id
    /* properties */
    , pts.player_season_teams
    , coalesce(min(pts.season_start_dt) over (partition by pts.player_id, pts.season_id) = pts.season_start_dt, false) as is_player_team_start_season
    , coalesce(max(pts.season_end_dt) over (partition by pts.player_id, pts.season_id) = pts.season_end_dt, false) as is_player_team_end_season
    , coalesce(max(pts.season_end_dt) over (partition by pts.player_id, pts.season_id) = pts.season_end_dt and player.is_active, false) as is_player_current_team
    , coalesce(not player.is_active, true) as is_player_retired
    , pts.games_played
    , pts.time_on_ice_minutes
    , pts.season_start_dt as player_season_team_first_dt
    , pts.season_end_dt as player_season_team_last_dt
    , min(pts.season_start_dt) over (partition by pts.player_id) as career_first_dt
    , max(pts.season_end_dt) over (partition by pts.player_id) as career_last_dt
from player_team_season as pts
left join {{ ref('d_players') }} as player on pts.player_id = player.player_id
