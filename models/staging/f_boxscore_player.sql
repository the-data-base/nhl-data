with

-- CTE1
home_player_team as (

    select
        hp.person.id as player_id
        , hp.person.rosterstatus as player_roster_status
        , 'Yes' as player_home_team
        , hp.position.code as player_position_code

        , hp.stats.playerstats.timeonice as player_toi
        , hp.stats.playerstats.goals as player_goals
        , hp.stats.playerstats.assists as player_assits
        , hp.stats.playerstats.shots as player_shots
        , hp.stats.playerstats.hits as player_hits
        , hp.stats.playerstats.powerplaygoals as player_powerplay_goals
        , hp.stats.playerstats.powerplayassists as player_powerplay_asissts
        , hp.stats.playerstats.penaltyminutes as player_penalty_minutes
        , hp.stats.playerstats.faceoffwins as player_faceoff_wins
        , hp.stats.playerstats.faceofftaken as player_faceoff_taken
        , hp.stats.playerstats.takeaways as player_takeaways
        , hp.stats.playerstats.giveaways as player_giveaways
        , hp.stats.playerstats.shorthandedgoals as player_shorthanded_goals
        , hp.stats.playerstats.shorthandedassists as player_shorthanded_assists
        , hp.stats.playerstats.blocked as player_blocked
        , hp.stats.playerstats.plusminus as player_plusminus
        , hp.stats.playerstats.eventimeonice as player_even_toi
        , hp.stats.playerstats.powerplaytimeonice as player_powerplay_toi
        , hp.stats.playerstats.shorthandedtimeonice as player_shorthanded_toi
        , hp.stats.playerstats.pim as player_pim
        , hp.stats.playerstats.saves as player_saves
        , hp.stats.playerstats.powerplaysaves as player_powerplay_saves
        , hp.stats.playerstats.shorthandedsaves as player_shorthanded_saves
        , hp.stats.playerstats.evensaves as player_even_saves
        , hp.stats.playerstats.shorthandedshotsagainst as player_shorthanded_shots_against
        , hp.stats.playerstats.evenshotsagainst as player_even_shots_against
        , hp.stats.playerstats.powerplayshotsagainst as player_powerplay_shots_against
        , hp.stats.playerstats.decision as player_decision
        , hp.stats.playerstats.savepercentage as player_save_percentage
        , hp.stats.playerstats.powerplaysavepercentage as player_powerplay_save_percentage
        , hp.stats.playerstats.evenstrengthsavepercentage as player_even_save_percentage

    from
        {{ref('live_boxscore')}}
    , unnest(b.teams.home.players) as hp


    where 1 = 1

)

-- CTE2
, away_player_team as (

    select
        ap.person.id as player_id
        , ap.person.rosterstatus as player_roster_status
        , 'No' as player_home_team
        , ap.position.code as player_position_code

        , ap.stats.playerstats.timeonice as player_toi
        , ap.stats.playerstats.goals as player_goals
        , ap.stats.playerstats.assists as player_assits
        , ap.stats.playerstats.shots as player_shots
        , ap.stats.playerstats.hits as player_hits
        , ap.stats.playerstats.powerplaygoals as player_powerplay_goals
        , ap.stats.playerstats.powerplayassists as player_powerplay_asissts
        , ap.stats.playerstats.penaltyminutes as player_penalty_minutes
        , ap.stats.playerstats.faceoffwins as player_faceoff_wins
        , ap.stats.playerstats.faceofftaken as player_faceoff_taken
        , ap.stats.playerstats.takeaways as player_takeaways
        , ap.stats.playerstats.giveaways as player_giveaways
        , ap.stats.playerstats.shorthandedgoals as player_shorthanded_goals
        , ap.stats.playerstats.shorthandedassists as player_shorthanded_assists
        , ap.stats.playerstats.blocked as player_blocked
        , ap.stats.playerstats.plusminus as player_plusminus
        , ap.stats.playerstats.eventimeonice as player_even_toi
        , ap.stats.playerstats.powerplaytimeonice as player_powerplay_toi
        , ap.stats.playerstats.shorthandedtimeonice as player_shorthanded_toi
        , ap.stats.playerstats.pim as player_pim
        , ap.stats.playerstats.saves as player_saves
        , ap.stats.playerstats.powerplaysaves as player_powerplay_saves
        , ap.stats.playerstats.shorthandedsaves as player_shorthanded_saves
        , ap.stats.playerstats.evensaves as player_even_saves
        , ap.stats.playerstats.shorthandedshotsagainst as player_shorthanded_shots_against
        , ap.stats.playerstats.evenshotsagainst as player_even_shots_against
        , ap.stats.playerstats.powerplayshotsagainst as player_powerplay_shots_against
        , ap.stats.playerstats.decision as player_decision
        , ap.stats.playerstats.savepercentage as player_save_percentage
        , ap.stats.playerstats.powerplaysavepercentage as player_powerplay_save_percentage
        , ap.stats.playerstats.evenstrengthsavepercentage as player_even_save_percentage

    from
        {{ref('live_boxscore')}}
    , unnest(b.teams.away.players) as ap

    where 1 = 1
)

-- CTE3
, player_team as (

    select
        hpt.*

    from
        home_player_team as hpt

    union all

    select
        apt.*
    from
        away_player_team as apt

)

-- Final
select
    /* Primary Key */
    concat(b.game_id, '_', pt.player_id) as id

    /* Foreign Keys */
    , b.game_id
    , pt.player_id
    , b.teams.away.team.id as away_team_id
    , b.teams.home.team.id as home_team_id

    /* Properties */
    -- Team level
    , pt.player_home_team
    , b.teams.away.teamstats.teamskaterstats.goals as away_team_goals
    , b.teams.away.teamstats.teamskaterstats.pim as away_team_pim
    , b.teams.away.teamstats.teamskaterstats.shots as away_team_shots
    , b.teams.away.teamstats.teamskaterstats.powerplaygoals as away_team_powerplay_goals
    , b.teams.away.teamstats.teamskaterstats.powerplayopportunities as away_team_powerplay_opportunities
    , b.teams.away.teamstats.teamskaterstats.faceoffwinpercentage as away_team_faceoff_percentage
    , b.teams.away.teamstats.teamskaterstats.blocked as away_team_blocked
    , b.teams.away.teamstats.teamskaterstats.takeaways as away_team_takeaways
    , b.teams.away.teamstats.teamskaterstats.giveaways as away_team_giveaways
    , b.teams.away.teamstats.teamskaterstats.hits as away_team_hits
    , b.teams.home.teamstats.teamskaterstats.goals as home_team_goals
    , b.teams.home.teamstats.teamskaterstats.pim as home_team_pim
    , b.teams.home.teamstats.teamskaterstats.shots as home_team_shots
    , b.teams.home.teamstats.teamskaterstats.powerplaygoals as home_team_powerplay_goals
    , b.teams.home.teamstats.teamskaterstats.powerplayopportunities as home_team_powerplay_opportunities
    , b.teams.home.teamstats.teamskaterstats.faceoffwinpercentage as home_team_faceoff_percentage
    , b.teams.home.teamstats.teamskaterstats.blocked as home_team_blocked
    , b.teams.home.teamstats.teamskaterstats.takeaways as home_team_takeaways
    , b.teams.home.teamstats.teamskaterstats.giveaways as home_team_giveaways
    , b.teams.home.teamstats.teamskaterstats.hits as home_team_hits
    -- Player level
    , pt.player_roster_status
    , pt.player_position_code
    , pt.player_toi
    , pt.player_goals
    , pt.player_assits
    , pt.player_shots
    , pt.player_hits
    , pt.player_powerplay_goals
    , pt.player_powerplay_asissts
    , pt.player_penalty_minutes
    , pt.player_faceoff_wins
    , pt.player_faceoff_taken
    , pt.player_takeaways
    , pt.player_giveaways
    , pt.player_shorthanded_goals
    , pt.player_shorthanded_assists
    , pt.player_blocked
    , pt.player_plusminus
    , pt.player_even_toi
    , pt.player_powerplay_toi
    , pt.player_shorthanded_toi
    , pt.player_pim
    , pt.player_saves
    , pt.player_powerplay_saves
    , pt.player_shorthanded_saves
    , pt.player_even_saves
    , pt.player_shorthanded_shots_against
    , pt.player_even_shots_against
    , pt.player_powerplay_shots_against
    , pt.player_decision
    , pt.player_save_percentage
    , pt.player_powerplay_save_percentage
    , pt.player_even_save_percentage

from
    {{ ref('live_boxscore') }} as b
, player_team as pt

where 1 = 1

order by
    b.game_id desc