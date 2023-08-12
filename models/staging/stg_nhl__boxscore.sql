with

deduped as (
    select * from {{ source('meltano', 'live_boxscore') }}
    qualify row_number() over( -- deduplicate gameids that were ingested more than once
        partition by
            gameid
    ) = 1
)

, home_boxscore as (
    select
        /* Primary Key */
        {{ dbt_utils.surrogate_key(['gameid', 'teams.home.team.id', 'players.person.id']) }} as stg_nhl__boxscore_id

        /* Foreign Keys */
        , gameid as game_id
        , teams.home.team.id as team_id
        , players.person.id as player_id

        /* Properties */
        -- Team properties
        , 'Home' as team_type
        , teams.home.team.name as team_name
        , teams.home.teamstats.teamskaterstats.blocked as team_blocked
        , teams.home.teamstats.teamskaterstats.faceoffwinpercentage as team_faceoff_percentage
        , teams.home.teamstats.teamskaterstats.giveaways as team_giveaways
        , teams.home.teamstats.teamskaterstats.goals as team_goals
        , teams.home.teamstats.teamskaterstats.hits as team_hits
        , teams.home.teamstats.teamskaterstats.pim as team_pim
        , teams.home.teamstats.teamskaterstats.powerplaygoals as team_powerplay_goals
        , teams.home.teamstats.teamskaterstats.powerplayopportunities as team_powerplay_opportunities
        , teams.home.teamstats.teamskaterstats.shots as team_shots
        , teams.home.teamstats.teamskaterstats.takeaways as team_takeaways
        , teams.home.scratches as team_scratches
        -- Player properties
        , players.person.fullname as player_full_name
        , players.person.rosterstatus as player_roster_status
        , players.position.code as player_position_code
        , players.stats.playerstats.assists as assists
        , players.stats.playerstats.blocked as blocked
        , players.stats.playerstats.decision as decision
        , players.stats.playerstats.evensaves as even_saves
        , players.stats.playerstats.evenshotsagainst as even_shots_against
        , players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage
        , players.stats.playerstats.eventimeonice as even_time_on_ice
        , players.stats.playerstats.faceofftaken as faceoff_taken
        , players.stats.playerstats.faceoffwins as faceoff_wins
        , players.stats.playerstats.giveaways as giveaways
        , players.stats.playerstats.goals as goals
        , players.stats.playerstats.hits as hits
        , players.stats.playerstats.penaltyminutes as penalty_minutes
        , players.stats.playerstats.pim as pim
        , players.stats.playerstats.plusminus as plus_minus
        , players.stats.playerstats.powerplayassists as powerplay_assists
        , players.stats.playerstats.powerplaygoals as powerplay_goals
        , players.stats.playerstats.powerplaysavepercentage as powerplay_save_percentage
        , players.stats.playerstats.powerplaysaves as powerplay_saves
        , players.stats.playerstats.powerplayshotsagainst as powerplay_shots_against
        , players.stats.playerstats.powerplaytimeonice as powerplay_time_on_ice
        , players.stats.playerstats.savepercentage as save_percentage
        , players.stats.playerstats.saves as saves
        , players.stats.playerstats.shorthandedassists as short_handed_assists
        , players.stats.playerstats.shorthandedgoals as short_handed_goals
        , players.stats.playerstats.shorthandedsaves as short_handed_saves
        , players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , players.stats.playerstats.shots as shots
        , players.stats.playerstats.takeaways as takeaways
        , players.stats.playerstats.timeonice as time_on_ice
    from deduped
    , unnest(teams.home.players) as players
)

, away_boxscore as (
    select
        /* Primary Key */
        {{ dbt_utils.surrogate_key(['gameid', 'teams.away.team.id', 'players.person.id']) }} as stg_nhl__boxscore_id

        /* Foreign Keys */
        , gameid as game_id
        , teams.away.team.id as team_id
        , players.person.id as player_id

        /* Properties */
        -- Team properties
        , 'Away' as team_type
        , teams.away.team.name as team_name
        , teams.away.teamstats.teamskaterstats.blocked as team_blocked
        , teams.away.teamstats.teamskaterstats.faceoffwinpercentage as team_faceoff_percentage
        , teams.away.teamstats.teamskaterstats.giveaways as team_giveaways
        , teams.away.teamstats.teamskaterstats.goals as team_goals
        , teams.away.teamstats.teamskaterstats.hits as team_hits
        , teams.away.teamstats.teamskaterstats.pim as team_pim
        , teams.away.teamstats.teamskaterstats.powerplaygoals as team_powerplay_goals
        , teams.away.teamstats.teamskaterstats.powerplayopportunities as team_powerplay_opportunities
        , teams.away.teamstats.teamskaterstats.shots as team_shots
        , teams.away.teamstats.teamskaterstats.takeaways as team_takeaways
        , teams.away.scratches as team_scratches
        -- Player properties
        , players.person.fullname as player_full_name
        , players.person.rosterstatus as player_roster_status
        , players.position.code as player_position_code
        , players.stats.playerstats.assists as assists
        , players.stats.playerstats.blocked as blocked
        , players.stats.playerstats.decision as decision
        , players.stats.playerstats.evensaves as even_saves
        , players.stats.playerstats.evenshotsagainst as even_shots_against
        , players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage
        , players.stats.playerstats.eventimeonice as even_time_on_ice
        , players.stats.playerstats.faceofftaken as faceoff_taken
        , players.stats.playerstats.faceoffwins as faceoff_wins
        , players.stats.playerstats.giveaways as giveaways
        , players.stats.playerstats.goals as goals
        , players.stats.playerstats.hits as hits
        , players.stats.playerstats.penaltyminutes as penalty_minutes
        , players.stats.playerstats.pim as pim
        , players.stats.playerstats.plusminus as plus_minus
        , players.stats.playerstats.powerplayassists as powerplay_assists
        , players.stats.playerstats.powerplaygoals as powerplay_goals
        , players.stats.playerstats.powerplaysavepercentage as powerplay_save_percentage
        , players.stats.playerstats.powerplaysaves as powerplay_saves
        , players.stats.playerstats.powerplayshotsagainst as powerplay_shots_against
        , players.stats.playerstats.powerplaytimeonice as powerplay_time_on_ice
        , players.stats.playerstats.savepercentage as save_percentage
        , players.stats.playerstats.saves as saves
        , players.stats.playerstats.shorthandedassists as short_handed_assists
        , players.stats.playerstats.shorthandedgoals as short_handed_goals
        , players.stats.playerstats.shorthandedsaves as short_handed_saves
        , players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , players.stats.playerstats.shots as shots
        , players.stats.playerstats.takeaways as takeaways
        , players.stats.playerstats.timeonice as time_on_ice
    from deduped
    , unnest(teams.away.players) as players
)

, unioned as (
    select * from home_boxscore
    union all
    select * from away_boxscore
)

select * from unioned
