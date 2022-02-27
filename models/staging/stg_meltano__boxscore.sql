
with

live_boxscore as (
    select * from {{ source('meltano', 'live_boxscore') }}
),

-- CTE1
home_team as (
    select
        /* Primary Key */
        {{ dbt_utils.surrogate_key(['live_boxscore.game_id']) }} as id

        /* Foreign Keys */
        , live_boxscore.game_id

        /* Properties */
        , 'Home' as team_type
        , true as is_home_player

        /* Player Properties */
        , players.person.id as player_id
        , players.person.fullname as full_name
        , players.person.shootscatches as shoots_catches
        , players.person.rosterstatus as roster_status
        , players.position.code as position_code
        , players.position.name as position_name
        , players.position.type as position_type
        , players.position.abbreviation as position_abbreviation

        /* Team stats*/
        , teams.home.team.id as team_id
        , teams.home.teamStats.teamSkaterStats.goals as team_goals
        , teams.home.teamStats.teamSkaterStats.pim as team_pim
        , teams.home.teamStats.teamSkaterStats.shots as team_shots
        , teams.home.teamStats.teamSkaterStats.powerPlayGoals as team_powerplay_goals
        , teams.home.teamStats.teamSkaterStats.powerPlayOpportunities as team_powerplay_opportunities
        , teams.home.teamStats.teamSkaterStats.faceOffWinPercentage as team_faceoff_percentage
        , teams.home.teamStats.teamSkaterStats.blocked as team_blocked
        , teams.home.teamStats.teamSkaterStats.takeaways as team_takeaways
        , teams.home.teamStats.teamSkaterStats.giveaways as team_giveaways
        , teams.home.teamStats.teamSkaterStats.hits as team_hits

        /* Player stats */
        , players.stats.playerstats.timeonice as time_on_ice
        , players.stats.playerstats.assists as assists
        , players.stats.playerstats.goals as goals
        , players.stats.playerstats.shots as shots
        , players.stats.playerstats.hits as hits
        , players.stats.playerstats.powerplaygoals as powerplay_goals
        , players.stats.playerstats.powerplayassists as powerplay_assists
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
        , players.stats.playerstats.powerplaytimeonice as powerplay_time_on_ice
        , players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , players.stats.playerstats.pim as pim
        , players.stats.playerstats.saves as saves
        , players.stats.playerstats.powerplaysaves as powerplay_saves
        , players.stats.playerstats.shorthandedsaves as short_handed_saves
        , players.stats.playerstats.evensaves as even_saves
        , players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , players.stats.playerstats.evenshotsagainst as even_shots_against
        , players.stats.playerstats.powerplayshotsagainst as powerplay_shots_against
        , players.stats.playerstats.decision as decision
        , players.stats.playerstats.savepercentage as save_percentage
        , players.stats.playerstats.powerplaysavepercentage as powerplay_save_percentage
        , players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage

    from

        live_boxscore,
        unnest(teams.home.players) as players
)

-- CTE2
, away_team as (
    select
        /* Primary Key */
        {{ dbt_utils.surrogate_key(['live_boxscore.game_id']) }} as id

        /* Foreign Keys */
        , live_boxscore.game_id

        /* Properties */
        , 'Away' as team_type
        , false as is_home_player

        /* Player Properties */
        , players.person.id as player_id
        , players.person.fullname as full_name
        , players.person.shootscatches as shoots_catches
        , players.person.rosterstatus as roster_status
        , players.position.code as position_code
        , players.position.name as position_name
        , players.position.type as position_type
        , players.position.abbreviation as position_abbreviation

        /* Team stats*/
        , teams.away.team.id as team_id
        , teams.away.teamStats.teamSkaterStats.goals as team_goals
        , teams.away.teamStats.teamSkaterStats.pim as team_pim
        , teams.away.teamStats.teamSkaterStats.shots as team_shots
        , teams.away.teamStats.teamSkaterStats.powerPlayGoals as team_powerplay_goals
        , teams.away.teamStats.teamSkaterStats.powerPlayOpportunities as team_powerplay_opportunities
        , teams.away.teamStats.teamSkaterStats.faceOffWinPercentage as team_faceoff_percentage
        , teams.away.teamStats.teamSkaterStats.blocked as team_blocked
        , teams.away.teamStats.teamSkaterStats.takeaways as team_takeaways
        , teams.away.teamStats.teamSkaterStats.giveaways as team_giveaways
        , teams.away.teamStats.teamSkaterStats.hits as team_hits

        /* Player stats */
        , players.stats.playerstats.timeonice as time_on_ice
        , players.stats.playerstats.assists as assists
        , players.stats.playerstats.goals as goals
        , players.stats.playerstats.shots as shots
        , players.stats.playerstats.hits as hits
        , players.stats.playerstats.powerplaygoals as powerplay_goals
        , players.stats.playerstats.powerplayassists as powerplay_assists
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
        , players.stats.playerstats.powerplaytimeonice as powerplay_time_on_ice
        , players.stats.playerstats.shorthandedtimeonice as short_handed_time_on_ice
        , players.stats.playerstats.pim as pim
        , players.stats.playerstats.saves as saves
        , players.stats.playerstats.powerplaysaves as powerplay_saves
        , players.stats.playerstats.shorthandedsaves as short_handed_saves
        , players.stats.playerstats.evensaves as even_saves
        , players.stats.playerstats.shorthandedshotsagainst as short_handed_shots_against
        , players.stats.playerstats.evenshotsagainst as even_shots_against
        , players.stats.playerstats.powerplayshotsagainst as powerplay_shots_against
        , players.stats.playerstats.decision as decision
        , players.stats.playerstats.savepercentage as save_percentage
        , players.stats.playerstats.powerplaysavepercentage as powerplay_save_percentage
        , players.stats.playerstats.evenstrengthsavepercentage as even_strength_save_percentage

    from

        live_boxscore,
        unnest(teams.away.players) as players
)

-- CTE3
, boxscore as (
    select * from home_team
    union all
    select * from away_team

)

-- FINAL
select *
from boxscore
