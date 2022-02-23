WITH 

-- CTE1
HOME_PLAYER_TEAM AS (
    
    SELECT 
        hp.person.id as player_id
        ,hp.person.rosterStatus as player_roster_status
        ,'Yes' as player_home_team
        ,hp.position.code as player_position_code

        ,hp.stats.playerStats.timeOnIce as player_toi
        ,hp.stats.playerStats.goals as player_goals
        ,hp.stats.playerStats.assists as player_assits
        ,hp.stats.playerStats.shots as player_shots
        ,hp.stats.playerStats.hits as player_hits
        ,hp.stats.playerStats.powerPlayGoals as player_powerplay_goals
        ,hp.stats.playerStats.powerPlayAssists as player_powerplay_asissts
        ,hp.stats.playerStats.penaltyMinutes as player_penalty_minutes
        ,hp.stats.playerStats.faceOffWins as player_faceoff_wins
        ,hp.stats.playerStats.faceoffTaken as player_faceoff_taken
        ,hp.stats.playerStats.takeaways as player_takeaways
        ,hp.stats.playerStats.giveaways as player_giveaways
        ,hp.stats.playerStats.shortHandedGoals as player_shorthanded_goals
        ,hp.stats.playerStats.shortHandedAssists as player_shorthanded_assists
        ,hp.stats.playerStats.blocked as player_blocked
        ,hp.stats.playerStats.plusMinus as player_plusminus
        ,hp.stats.playerStats.evenTimeOnIce as player_even_toi
        ,hp.stats.playerStats.powerPlayTimeOnIce as player_powerplay_toi
        ,hp.stats.playerStats.shortHandedTimeOnIce as player_shorthanded_toi
        ,hp.stats.playerStats.pim as player_pim
        ,hp.stats.playerStats.saves as player_saves
        ,hp.stats.playerStats.powerPlaySaves as player_powerplay_saves
        ,hp.stats.playerStats.shortHandedSaves as player_shorthanded_saves
        ,hp.stats.playerStats.evenSaves as player_even_saves
        ,hp.stats.playerStats.shortHandedShotsAgainst as player_shorthanded_shots_against
        ,hp.stats.playerStats.evenShotsAgainst as player_even_shots_against
        ,hp.stats.playerStats.powerPlayShotsAgainst as player_powerplay_shots_against
        ,hp.stats.playerStats.decision as player_decision
        ,hp.stats.playerStats.savePercentage as player_save_percentage
        ,hp.stats.playerStats.powerPlaySavePercentage as player_powerplay_save_percentage
        ,hp.stats.playerStats.evenStrengthSavePercentage as player_even_save_percentage
    
    FROM 
        {{ref('live_boxscore')}} as b
        ,UNNEST(b.teams.home.players) as hp


    WHERE 1 = 1

  )

-- CTE2
, AWAY_PLAYER_TEAM AS (
    
    SELECT 
        ap.person.id as player_id
        ,ap.person.rosterStatus as player_roster_status
        ,'No' as player_home_team
        ,ap.position.code as player_position_code

        ,ap.stats.playerStats.timeOnIce as player_toi
        ,ap.stats.playerStats.goals as player_goals
        ,ap.stats.playerStats.assists as player_assits
        ,ap.stats.playerStats.shots as player_shots
        ,ap.stats.playerStats.hits as player_hits
        ,ap.stats.playerStats.powerPlayGoals as player_powerplay_goals
        ,ap.stats.playerStats.powerPlayAssists as player_powerplay_asissts
        ,ap.stats.playerStats.penaltyMinutes as player_penalty_minutes
        ,ap.stats.playerStats.faceOffWins as player_faceoff_wins
        ,ap.stats.playerStats.faceoffTaken as player_faceoff_taken
        ,ap.stats.playerStats.takeaways as player_takeaways
        ,ap.stats.playerStats.giveaways as player_giveaways
        ,ap.stats.playerStats.shortHandedGoals as player_shorthanded_goals
        ,ap.stats.playerStats.shortHandedAssists as player_shorthanded_assists
        ,ap.stats.playerStats.blocked as player_blocked
        ,ap.stats.playerStats.plusMinus as player_plusminus
        ,ap.stats.playerStats.evenTimeOnIce as player_even_toi
        ,ap.stats.playerStats.powerPlayTimeOnIce as player_powerplay_toi
        ,ap.stats.playerStats.shortHandedTimeOnIce as player_shorthanded_toi
        ,ap.stats.playerStats.pim as player_pim
        ,ap.stats.playerStats.saves as player_saves
        ,ap.stats.playerStats.powerPlaySaves as player_powerplay_saves
        ,ap.stats.playerStats.shortHandedSaves as player_shorthanded_saves
        ,ap.stats.playerStats.evenSaves as player_even_saves
        ,ap.stats.playerStats.shortHandedShotsAgainst as player_shorthanded_shots_against
        ,ap.stats.playerStats.evenShotsAgainst as player_even_shots_against
        ,ap.stats.playerStats.powerPlayShotsAgainst as player_powerplay_shots_against
        ,ap.stats.playerStats.decision as player_decision
        ,ap.stats.playerStats.savePercentage as player_save_percentage
        ,ap.stats.playerStats.powerPlaySavePercentage as player_powerplay_save_percentage
        ,ap.stats.playerStats.evenStrengthSavePercentage as player_even_save_percentage
    
    FROM 
        {{ref('live_boxscore')}} as b
        ,UNNEST(b.teams.away.players) as ap

    WHERE 1 = 1
)

-- CTE3
, PLAYER_TEAM AS (

    SELECT
       hpt.*

    FROM
        HOME_PLAYER_TEAM hpt

    UNION ALL

    SELECT
        apt.*
    FROM
        AWAY_PLAYER_TEAM apt

)

-- Final
SELECT 
    /* Primary Key */
    CONCAT(b.game_id,"_",pt.player_id) as id
    
    /* Foreign Keys */
    ,b.game_id
    ,pt.player_id
    ,b.teams.away.team.id as away_team_id
    ,b.teams.home.team.id as home_team_id

    /* Properties */
    -- Team level
    ,pt.player_home_team
    ,b.teams.away.teamStats.teamSkaterStats.goals as away_team_goals
    ,b.teams.away.teamStats.teamSkaterStats.pim as away_team_pim
    ,b.teams.away.teamStats.teamSkaterStats.shots as away_team_shots
    ,b.teams.away.teamStats.teamSkaterStats.powerPlayGoals as away_team_powerplay_goals
    ,b.teams.away.teamStats.teamSkaterStats.powerPlayOpportunities as away_team_powerplay_opportunities
    ,b.teams.away.teamStats.teamSkaterStats.faceOffWinPercentage as away_team_faceoff_percentage
    ,b.teams.away.teamStats.teamSkaterStats.blocked as away_team_blocked
    ,b.teams.away.teamStats.teamSkaterStats.takeaways as away_team_takeaways
    ,b.teams.away.teamStats.teamSkaterStats.giveaways as away_team_giveaways
    ,b.teams.away.teamStats.teamSkaterStats.hits as away_team_hits
    ,b.teams.home.teamStats.teamSkaterStats.goals as home_team_goals
    ,b.teams.home.teamStats.teamSkaterStats.pim as home_team_pim
    ,b.teams.home.teamStats.teamSkaterStats.shots as home_team_shots
    ,b.teams.home.teamStats.teamSkaterStats.powerPlayGoals as home_team_powerplay_goals
    ,b.teams.home.teamStats.teamSkaterStats.powerPlayOpportunities as home_team_powerplay_opportunities
    ,b.teams.home.teamStats.teamSkaterStats.faceOffWinPercentage as home_team_faceoff_percentage
    ,b.teams.home.teamStats.teamSkaterStats.blocked as home_team_blocked
    ,b.teams.home.teamStats.teamSkaterStats.takeaways as home_team_takeaways
    ,b.teams.home.teamStats.teamSkaterStats.giveaways as home_team_giveaways
    ,b.teams.home.teamStats.teamSkaterStats.hits as home_team_hits
    -- Player level
    ,pt.player_roster_status
    ,pt.player_position_code
    ,pt.player_toi
    ,pt.player_goals
    ,pt.player_assits
    ,pt.player_shots
    ,pt.player_hits
    ,pt.player_powerplay_goals
    ,pt.player_powerplay_asissts
    ,pt.player_penalty_minutes
    ,pt.player_faceoff_wins
    ,pt.player_faceoff_taken
    ,pt.player_takeaways
    ,pt.player_giveaways
    ,pt.player_shorthanded_goals
    ,pt.player_shorthanded_assists
    ,pt.player_blocked
    ,pt.player_plusminus
    ,pt.player_even_toi
    ,pt.player_powerplay_toi
    ,pt.player_shorthanded_toi
    ,pt.player_pim
    ,pt.player_saves
    ,pt.player_powerplay_saves
    ,pt.player_shorthanded_saves
    ,pt.player_even_saves
    ,pt.player_shorthanded_shots_against
    ,pt.player_even_shots_against
    ,pt.player_powerplay_shots_against
    ,pt.player_decision
    ,pt.player_save_percentage
    ,pt.player_powerplay_save_percentage
    ,pt.player_even_save_percentage

FROM 
    {{ref('live_boxscore')}} as b
    ,PLAYER_TEAM as pt

WHERE 1 = 1

ORDER BY 
    b.game_id desc