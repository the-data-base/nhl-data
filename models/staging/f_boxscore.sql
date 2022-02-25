with home_team as (
    select
        /* Primary Key */
        boxscore.game_id as id
        /* Properties */
        , 'Home' as team_type

        /* Player Properties */
        , players.person.fullname as full_name
        , players.person.shootscatches as shoots_catches
        , players.person.rosterstatus as roster_status
        , players.position.code as position_code
        , players.position.name as position_name
        , players.position.type as position_type
        , players.position.abbreviation as position_abbreviation

        /* Player stats */
        , players.stats.playerstats.timeonice as time_on_ice
        , players.stats.playerstats.assists as assists
        , players.stats.playerstats.goals as goals
        , players.stats.playerstats.shots as shots
        , players.stats.playerstats.hits as hits
        , players.stats.playerstats.powerplaygoals as power_play_goals
        , players.stats.playerstats.powerplayassists as power_play_assists
        , players.stats.playerstats.penaltyminutes as penalty_minutes
        , players.stats.playerstats.faceoffwins as faceoff_wins
        , players.stats.playerstats.faceofftaken as faceoff_taken
        , players.stats.playerstats.takeaways as takeaways
        , players.stats.playerstats.giveaways as giveaways
        , players.stats.playerstats.shorthandedgoals as short_handed_goals
        , players.stats.playerstats.shorthandedassists as short_handed_assists
        , players.stats.playerstats.blocked as blocked
        , players.stats.playerstats.plusminus as plus_minus
        , players.stats.playerstats.eventimeonice as even_time_on_ice
        , players.stats.playerstats.powerplaytimeonice as power_play_time_on_ice
        , players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , players.stats.playerstats.pim as pim
        , players.stats.playerstats.saves as saves
        , players.stats.playerstats.powerplaysaves as power_play_saves
        , players.stats.playerstats.shorthandedsaves as short_handed_saves
        , players.stats.playerstats.evensaves as even_saves
        , players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , players.stats.playerstats.evenshotsagainst as even_shots_against
        , players.stats.playerstats.powerplayshotsagainst as power_play_shots_against
        , players.stats.playerstats.decision as decision
        , players.stats.playerstats.savepercentage as save_percentage
        , players.stats.playerstats.powerplaysavepercentage as power_play_save_percentage
        , players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage

    from {{ref('live_boxscore')}} as boxscore,
        unnest(teams.home.players) as players
)

, away_team as (
    select
        /* Primary Key */
        boxscore.game_id as id
        /* Properties */
        , 'Away' as team_type

        /* Player Properties */
        , players.person.fullname as full_name
        , players.person.shootscatches as shoots_catches
        , players.person.rosterstatus as roster_status
        , players.position.code as position_code
        , players.position.name as position_name
        , players.position.type as position_type
        , players.position.abbreviation as position_abbreviation

        /* Player stats */
        , players.stats.playerstats.timeonice as time_on_ice
        , players.stats.playerstats.assists as assists
        , players.stats.playerstats.goals as goals
        , players.stats.playerstats.shots as shots
        , players.stats.playerstats.hits as hits
        , players.stats.playerstats.powerplaygoals as power_play_goals
        , players.stats.playerstats.powerplayassists as power_play_assists
        , players.stats.playerstats.penaltyminutes as penalty_minutes
        , players.stats.playerstats.faceoffwins as faceoff_wins
        , players.stats.playerstats.faceofftaken as faceoff_taken
        , players.stats.playerstats.takeaways as takeaways
        , players.stats.playerstats.giveaways as giveaways
        , players.stats.playerstats.shorthandedgoals as short_handed_goals
        , players.stats.playerstats.shorthandedassists as short_handed_assists
        , players.stats.playerstats.blocked as blocked
        , players.stats.playerstats.plusminus as plus_minus
        , players.stats.playerstats.eventimeonice as even_time_on_ice
        , players.stats.playerstats.powerplaytimeonice as power_play_time_on_ice
        , players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , players.stats.playerstats.pim as pim
        , players.stats.playerstats.saves as saves
        , players.stats.playerstats.powerplaysaves as power_play_saves
        , players.stats.playerstats.shorthandedsaves as short_handed_saves
        , players.stats.playerstats.evensaves as even_saves
        , players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , players.stats.playerstats.evenshotsagainst as even_shots_against
        , players.stats.playerstats.powerplayshotsagainst as power_play_shots_against
        , players.stats.playerstats.decision as decision
        , players.stats.playerstats.savepercentage as save_percentage
        , players.stats.playerstats.powerplaysavepercentage as power_play_save_percentage
        , players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage

    from {{ ref('live_boxscore') }} as boxscore,
        unnest(teams.away.players) as players
)

, boxscore as (
    select * from home_team
    union all
    select * from away_team

)

select * from boxscore