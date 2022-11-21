with
-- CTE1
live_boxscore as (
    select * from {{ source('meltano', 'live_boxscore') }}
)

-- CTE2
, home_team_player as (
    select
        /* Identifiers */
        gameid as game_id
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

    from live_boxscore
    , unnest(teams.home.players) as home_players

)

-- CTE3
, away_team_player as (
    select
        /* Identifiers */
        live_boxscore.gameid as game_id
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

    from live_boxscore
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
    {{ dbt_utils.surrogate_key(['game_id', 'team_id', 'player_id']) }} as stg_nhl__boxscore_player_id

    /* Identifiers */
    , game_id
    , team_id
    , player_id

    /* Properties */
    , team_name
    , team_type

    /* Player stats */
    , player_full_name
    , player_roster_status
    , player_position_code
    , time_on_ice
    , assists
    , goals
    , shots
    , hits
    , powerplay_goals
    , powerplay_assists
    , penalty_minutes
    , faceoff_wins
    , faceoff_taken
    , takeaways
    , giveaways
    , short_handed_goals
    , short_handed_assists
    , blocked
    , plus_minus
    , even_time_on_ice
    , powerplay_time_on_ice
    , short_handed_time_on_ice
    , pim
    , saves
    , powerplay_saves
    , short_handed_saves
    , even_saves
    , short_handed_shots_against
    , even_shots_against
    , powerplay_shots_against
    , decision
    , save_percentage
    , powerplay_save_percentage
    , even_strength_save_percentage

from boxscore_player
qualify row_number() over(
    partition by
        game_id
        , team_id
        , player_id
) = 1
