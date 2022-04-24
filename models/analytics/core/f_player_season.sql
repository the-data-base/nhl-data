with 
player_season as (
    select
        bp.player_id
        ,bp.player_full_name
        ,season.id as season_id
        ,season.regular_season_start_date
        ,season.regular_season_end_date
        ,season.number_of_games full_season_games
        ,count(distinct bp.game_id) as boxscore_games
        ,sum(ifnull(bp.assists, 0)) as assists
        ,sum(ifnull(bp.goals, 0)) as goals
        ,sum(ifnull(bp.shots, 0)) as shots
        ,sum(ifnull(bp.hits, 0)) as hits
        ,sum(bp.faceoff_wins) as faceoff_wins
        ,sum(bp.faceoff_taken) as faceoff_taken
        ,sum(ifnull(bp.takeaways, 0)) as takeaways
        ,sum(ifnull(bp.giveaways, 0)) as giveaways
        ,sum(ifnull(bp.blocked,0)) as blocked
        ,sum(ifnull(bp.plus_minus,0)) as plus_minus
        ,sum(ifnull(bp.pim, 0)) as pim
        ,sum(cast(split(bp.time_on_ice, ':')[OFFSET(0)] as int)) as time_on_ice_mins
        ,sum(cast(split(bp.time_on_ice, ':')[OFFSET(1)] as int)) as time_on_ice_seconds
        ,sum(ifnull(bp.powerplay_goals, 0)) as powerplay_goals
        ,sum(ifnull(bp.powerplay_assists, 0)) as powerplay_assists
        ,sum(ifnull(bp.short_handed_goals, 0)) as short_handed_goals
        ,sum(ifnull(bp.short_handed_assists, 0)) as short_handed_assists
        ,sum(ifnull(bp.saves, 0)) as saves
        ,sum(ifnull(bp.powerplay_saves, 0)) as powerplay_saves
        ,sum(ifnull(bp.even_saves, 0)) as even_saves
        ,sum(ifnull(bp.short_handed_shots_against, 0)) as short_handed_shots_against
        ,sum(ifnull(bp.even_shots_against, 0)) as even_shots_againstrplay_saves
        ,sum(ifnull(bp.powerplay_shots_against, 0)) as powerplay_shots_against
        --,sum(case when bp.decision = "W" then 1 else 0) as wins
        --,sum(case when bp.decision = "L" then 1 else 0) as losses
    from
        nhl-breakouts.dbt_dom.f_boxscore_player as bp
        left join nhl-breakouts.dbt_dom.d_schedule as schedule on schedule.game_id = bp.game_id
        left join nhl-breakouts.dbt_dom.d_seasons as season on season.id = schedule.season_id
    group by
        bp.player_id
        ,bp.player_full_name
        ,season.id
        ,season.number_of_games
        ,season.regular_season_start_date
        ,season.regular_season_end_date
    order by
        count(distinct bp.game_id) desc
        ,sum(bp.goals) desc
)

select 
    player_season.player_id
    ,player_season.player_full_name
    ,player_season.season_id
    ,player_season.regular_season_start_date
    ,player_season.regular_season_end_date
    ,player_season.full_season_games
    ,player_season.boxscore_games
    ,player_season.goals
    ,player_season.assists
    ,player_season.goals + player_season.assists as points
    ,player_season.shots
    ,player_season.hits
    ,player_season.faceoff_wins
    ,player_season.faceoff_taken
    ,player_season.takeaways
    ,player_season.giveaways    
    ,player_season.blocked
    ,player_season.plus_minus
    ,player_season.pim    
    ,player_season.powerplay_goals
    ,player_season.powerplay_assists
    ,player_season.short_handed_goals
    ,player_season.short_handed_assists
    ,player_season.saves
    ,player_season.powerplay_saves
    ,player_season.even_saves
    ,player_season.short_handed_shots_against
    ,player_season.even_shots_againstrplay_saves
    ,player_season.powerplay_shots_against
    --,player_season.wins
    --,player_season.losses
    ,(player_season.time_on_ice_mins * 60) + (player_season.time_on_ice_mins) as time_on_ice_seconds
from 
    player_season;
