with home_team as (
    select
        /* Primary Key */
        boxscore.game_id as id
        /* Properties */
        , 'Home' as team_type

        /* Player Properties */
        , players.person.fullName as full_name
        , players.person.shootsCatches as shoots_catches
        , players.person.rosterStatus as roster_status
        , players.position.code as position_code
        , players.position.name as position_name
        , players.position.type as position_type
        , players.position.abbreviation as position_abbreviation

        /* Player stats */
        , players.stats.playerStats.timeOnIce as time_on_ice
        , players.stats.playerStats.assists as assists
        , players.stats.playerStats.goals as goals
        , players.stats.playerStats.shots as shots
        , players.stats.playerStats.hits as hits
        , players.stats.playerStats.powerPlayGoals as power_play_goals
        , players.stats.playerStats.powerPlayAssists as power_play_assists
        , players.stats.playerStats.penaltyMinutes as penalty_minutes
        , players.stats.playerStats.faceOffWins as faceoff_wins
        , players.stats.playerStats.faceoffTaken as faceoff_taken
        , players.stats.playerStats.takeaways as takeaways
        , players.stats.playerStats.giveaways as giveaways
        , players.stats.playerStats.shortHandedGoals as short_handed_goals
        , players.stats.playerStats.shortHandedAssists as short_handed_assists
        , players.stats.playerStats.blocked as blocked
        , players.stats.playerStats.plusMinus as plus_minus
        , players.stats.playerStats.evenTimeOnIce as even_time_on_ice
        , players.stats.playerStats.powerPlayTimeOnIce as power_play_time_on_ice
        , players.stats.playerStats.shortHandedTimeOnIce as short_handed_time_on_ice
        , players.stats.playerStats.pim as pim
        , players.stats.playerStats.saves as saves
        , players.stats.playerStats.powerPlaySaves as power_play_saves
        , players.stats.playerStats.shortHandedSaves as short_handed_saves
        , players.stats.playerStats.evenSaves as even_saves
        , players.stats.playerStats.shortHandedShotsAgainst as short_handed_shots_against
        , players.stats.playerStats.evenShotsAgainst as even_shots_against
        , players.stats.playerStats.powerPlayShotsAgainst as power_play_shots_against
        , players.stats.playerStats.decision as decision
        , players.stats.playerStats.savePercentage as save_percentage
        , players.stats.playerStats.powerPlaySavePercentage as power_play_save_percentage
        , players.stats.playerStats.evenStrengthSavePercentage as even_strength_save_percentage

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
        , players.person.fullName as full_name
        , players.person.shootsCatches as shoots_catches
        , players.person.rosterStatus as roster_status
        , players.position.code as position_code
        , players.position.name as position_name
        , players.position.type as position_type
        , players.position.abbreviation as position_abbreviation

        /* Player stats */
        , players.stats.playerStats.timeOnIce as time_on_ice
        , players.stats.playerStats.assists as assists
        , players.stats.playerStats.goals as goals
        , players.stats.playerStats.shots as shots
        , players.stats.playerStats.hits as hits
        , players.stats.playerStats.powerPlayGoals as power_play_goals
        , players.stats.playerStats.powerPlayAssists as power_play_assists
        , players.stats.playerStats.penaltyMinutes as penalty_minutes
        , players.stats.playerStats.faceOffWins as faceoff_wins
        , players.stats.playerStats.faceoffTaken as faceoff_taken
        , players.stats.playerStats.takeaways as takeaways
        , players.stats.playerStats.giveaways as giveaways
        , players.stats.playerStats.shortHandedGoals as short_handed_goals
        , players.stats.playerStats.shortHandedAssists as short_handed_assists
        , players.stats.playerStats.blocked as blocked
        , players.stats.playerStats.plusMinus as plus_minus
        , players.stats.playerStats.evenTimeOnIce as even_time_on_ice
        , players.stats.playerStats.powerPlayTimeOnIce as power_play_time_on_ice
        , players.stats.playerStats.shortHandedTimeOnIce as short_handed_time_on_ice
        , players.stats.playerStats.pim as pim
        , players.stats.playerStats.saves as saves
        , players.stats.playerStats.powerPlaySaves as power_play_saves
        , players.stats.playerStats.shortHandedSaves as short_handed_saves
        , players.stats.playerStats.evenSaves as even_saves
        , players.stats.playerStats.shortHandedShotsAgainst as short_handed_shots_against
        , players.stats.playerStats.evenShotsAgainst as even_shots_against
        , players.stats.playerStats.powerPlayShotsAgainst as power_play_shots_against
        , players.stats.playerStats.decision as decision
        , players.stats.playerStats.savePercentage as save_percentage
        , players.stats.playerStats.powerPlaySavePercentage as power_play_save_percentage
        , players.stats.playerStats.evenStrengthSavePercentage as even_strength_save_percentage

    from {{ref('live_boxscore')}} as boxscore,
    unnest(teams.away.players) as players
)

, boxscore as (
    select * from home_team
    union all 
    select * from away_team

)
select * from boxscore;