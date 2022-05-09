with

home_team_scratches as (
    select
        boxscore.game_id
        , scratches as player_id
    from {{ ref('stg_nhl__boxscore') }} as boxscore
    , unnest(boxscore.home_team_scratches) as scratches
)

, away_team_scratches as (
    select
        boxscore.game_id
        , scratches as player_id
    from {{ ref('stg_nhl__boxscore') }} as boxscore
    , unnest(boxscore.away_team_scratches) as scratches
)

, unioned as (
    select
        game_id
        , player_id
    from home_team_scratches

    union all

    select
        game_id
        , player_id
    from away_team_scratches
)

select * from unioned
