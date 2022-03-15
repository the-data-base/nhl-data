with
-- CTE1
live_boxscore as (
    select  
        * 
    from    
        {{ source('meltano', 'live_boxscore') }}
    )

-- CTE2
home_team_player as (
    select
        /* Primary Key */
        concat(live_boxscore.game_id, teams.home.team.id, home_players.person.id) as id
        
        /* Foreign Keys */
        , live_boxscore.game_id
        , teams.home.team.id as team_id
        , home_players.person.id as player_id

        /* Properties */
        , teams.home.team.name as team_name
        , 'Home' as team_type

        /* Player stats */
        , home_players.person.fullname as player_full_name
        , home_players.person.rosterstatus as player_roster_status
        , home_players.position.code as player_position_code
        , home_players.stats.playerstats.timeonice as time_on_ice
        , home_players.stats.playerstats.assists as assists
        , home_players.stats.playerstats.goals as goals
        , home_players.stats.playerstats.shots as shots
        , home_players.stats.playerstats.hits as hits
        , home_players.stats.playerstats.powerplaygoals as powerplay_goals
        , home_players.stats.playerstats.powerplayassists as powerplay_assists
        , home_players.stats.playerstats.penaltyminutes as penalty_minutes
        , home_players.stats.playerstats.faceoffwins as faceoff_wins
        , home_players.stats.playerstats.faceofftaken as faceoff_taken
        , home_players.stats.playerstats.takeaways as takeaways
        , home_players.stats.playerstats.giveaways as giveaways
        , home_players.stats.playerstats.shorthandedgoals as short_handed_goals
        , home_players.stats.playerstats.shorthandedassists as short_handed_assists
        , home_players.stats.playerstats.blocked as blocked
        , home_players.stats.playerstats.plusminus as plus_minus
        , home_players.stats.playerstats.eventimeonice as even_time_on_ice
        , home_players.stats.playerstats.powerplaytimeonice as powerplay_time_on_ice
        , home_players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , home_players.stats.playerstats.pim as pim
        , home_players.stats.playerstats.saves as saves
        , home_players.stats.playerstats.powerplaysaves as powerplay_saves
        , home_players.stats.playerstats.shorthandedsaves as short_handed_saves
        , home_players.stats.playerstats.evensaves as even_saves
        , home_players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , home_players.stats.playerstats.evenshotsagainst as even_shots_against
        , home_players.stats.playerstats.powerplayshotsagainst as powerplay_shots_against
        , home_players.stats.playerstats.decision as decision
        , home_players.stats.playerstats.savepercentage as save_percentage
        , home_players.stats.playerstats.powerplaysavepercentage as powerplay_save_percentage
        , home_players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage

    from
        live_boxscore
        , unnest(teams.home.players) as home_players

        )

-- CTE3
, away_team_player as (
    select
        /* Primary Key */
        concat(live_boxscore.game_id, teams.away.team.id, away_players.person.id) as id

        /* Foreign Keys */
        , live_boxscore.game_id
        , teams.away.team.id as team_id
        , away_players.person.id as player_id


        /* Properties */
        , teams.away.team.name as team_name
        , 'Away' as team_type

        /* Player stats */
        , away_players.person.fullname as player_full_name
        , away_players.person.rosterstatus as player_roster_status
        , away_players.position.code as player_position_code
        , away_players.stats.playerstats.timeonice as time_on_ice
        , away_players.stats.playerstats.assists as assists
        , away_players.stats.playerstats.goals as goals
        , away_players.stats.playerstats.shots as shots
        , away_players.stats.playerstats.hits as hits
        , away_players.stats.playerstats.powerplaygoals as powerplay_goals
        , away_players.stats.playerstats.powerplayassists as powerplay_assists
        , away_players.stats.playerstats.penaltyminutes as penalty_minutes
        , away_players.stats.playerstats.faceoffwins as faceoff_wins
        , away_players.stats.playerstats.faceofftaken as faceoff_taken
        , away_players.stats.playerstats.takeaways as takeaways
        , away_players.stats.playerstats.giveaways as giveaways
        , away_players.stats.playerstats.shorthandedgoals as short_handed_goals
        , away_players.stats.playerstats.shorthandedassists as short_handed_assists
        , away_players.stats.playerstats.blocked as blocked
        , away_players.stats.playerstats.plusminus as plus_minus
        , away_players.stats.playerstats.eventimeonice as even_time_on_ice
        , away_players.stats.playerstats.powerplaytimeonice as powerplay_time_on_ice
        , away_players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , away_players.stats.playerstats.pim as pim
        , away_players.stats.playerstats.saves as saves
        , away_players.stats.playerstats.powerplaysaves as powerplay_saves
        , away_players.stats.playerstats.shorthandedsaves as short_handed_saves
        , away_players.stats.playerstats.evensaves as even_saves
        , away_players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , away_players.stats.playerstats.evenshotsagainst as even_shots_against
        , away_players.stats.playerstats.powerplayshotsagainst as powerplay_shots_against
        , away_players.stats.playerstats.decision as decision
        , away_players.stats.playerstats.savepercentage as save_percentage
        , away_players.stats.playerstats.powerplaysavepercentage as powerplay_save_percentage
        , away_players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage

    from
        live_boxscore
        , unnest(teams.away.players) as away_players

        )

-- CTE4
, boxscore_player as (
    select * from home_team_player
    union all
    select * from away_team_player

)

-- Final query, return everything
select  
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['boxscore_player.id']) }} as id

    /* Foreign Keys */
    , boxscore_player.game_id
    , boxscore_player.team_id
    , boxscore_player.player_id

    /* Properties */
    , boxscore_player.team_name
    , boxscore_player.team_type

    /* Player stats */
    , boxscore_player.player_full_name
    , boxscore_player.player_roster_status
    , boxscore_player.player_position_code
    , boxscore_player.time_on_ice
    , boxscore_player.assists
    , boxscore_player.goals
    , boxscore_player.shots
    , boxscore_player.hits
    , boxscore_player.powerplay_goals
    , boxscore_player.powerplay_assists
    , boxscore_player.penalty_minutes
    , boxscore_player.faceoff_wins
    , boxscore_player.faceoff_taken
    , boxscore_player.takeaways
    , boxscore_player.giveaways
    , boxscore_player.short_handed_goals
    , boxscore_player.short_handed_assists
    , boxscore_player.blocked
    , boxscore_player.plus_minus
    , boxscore_player.even_time_on_ice
    , boxscore_player.powerplay_time_on_ice
    , boxscore_player.short_handed_time_on_ice
    , boxscore_player.pim
    , boxscore_player.saves
    , boxscore_player.powerplay_saves
    , boxscore_player.short_handed_saves
    , boxscore_player.even_saves
    , boxscore_player.short_handed_shots_against
    , boxscore_player.even_shots_against
    , boxscore_player.powerplay_shots_against
    , boxscore_player.decision
    , boxscore_player.save_percentage
    , boxscore_player.powerplay_save_percentage
    , boxscore_player.even_strength_save_percentage

from     
    boxscore_player

order by
    boxscore_player.game_id desc
    , boxscore_player.team_id desc
    , boxscore_player.player_id  desc