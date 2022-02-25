select
    /* Primary Key */
    b.game_id as id

    /* Foreign Keys */
    , b.teams.away.team.id as away_team_id
    , b.teams.home.team.id as home_team_id

    /* Properties */
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

from
    {{ ref('live_boxscore') }} as b

where 1 = 1

order by
    b.game_id desc