# Calculate the distance the trail 'retreats' at the end of a possession.
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

source("scripts/helper.R")

trail_retreating_query <-
    "
WITH reference_times AS (
SELECT
    gameDate,
    gameId,
    prior_poss_num AS possNum,
    basketX,
    prior_event_type,
    eventType,
    prior_event_time,
    wcStart,
FROM (
    SELECT
        poss.gameDate,
        poss.gameId,
        poss.possNum,
    LAG(poss.basketX, 1) OVER (
        PARTITION BY event.gameDate, event.gameId
        ORDER BY event.gameDate, event.gameId, event.wcTime) AS basketX,
    LAG(event.eventType, 1) OVER (
        PARTITION BY event.gameDate, event.gameId
        ORDER BY event.gameDate, event.gameId, event.wcTime) AS prior_event_type,
    event.eventType,
    LAG(event.wcTime, 1) OVER (
        PARTITION BY event.gameDate, event.gameId
        ORDER BY event.gameDate, event.gameId, event.wcTime) AS prior_event_time,
    poss.wcStart,
    LAG(poss.possNum, 1) OVER (
        PARTITION BY event.gameDate, event.gameId
        ORDER BY event.gameDate, event.gameId, event.wcTime) AS prior_poss_num,
FROM 
    `nba-tracking-data.NbaPlayerTracking.Events` event
INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
    event.gameDate = poss.gameDate
    AND event.gameId = poss.gameId
    AND event.possId = poss.possId
)
WHERE
prior_poss_num = possNum - 1
AND prior_event_time <> wcStart
)
SELECT
    reference_times.*,
    curr.playerId,
    ABS(reference_times.basketX - prior.x) AS basket_disk,
    ABS(curr.x - prior.x) AS shift_distance,
    ABS(
        reference_times.wcStart - reference_times.prior_event_time
    )/1000 shift_time
FROM 
    reference_times
INNER JOIN `nba-tracking-data.NbaPlayerTracking.Tracking` prior ON
    prior.gameDate = reference_times.gameDate
    AND prior.gameId = reference_times.gameId
    AND prior.wcTime = reference_times.prior_event_time
INNER JOIN  `nba-tracking-data.NbaPlayerTracking.Tracking` curr ON
    curr.gameDate = reference_times.gameDate
    AND curr.gameId = reference_times.gameId
    AND curr.wcTime = reference_times.wcStart
    AND curr.playerId = prior.playerId
WHERE
    curr.teamId = 0
    AND reference_times.prior_event_type NOT IN ('FT', 'REP', 'TMO', 'JMP')
    AND reference_times.eventType NOT IN ('FT', 'REP', 'TMO', 'JMP')
ORDER BY
    reference_times.gameDate,
    reference_times.gameId,
    reference_times.wcStart
    "
    
trail_retreating_stat <-
    dbGetQuery(gbq, trail_retreating_query) %>%
    group_by(gameId, possNum) %>%
    slice(which.max(basket_disk)) %>%
    filter(shift_time <= 5.0) %>%
    mutate(shifted = ifelse(shift_distance >= 3.0, 1, 0))

game_stat <-
    trail_retreating_stat %>%
    group_by(gameId, playerId) %>%
    summarise(perc_poss_completed = 1- sum(shifted)/n())

season_stat <-
    trail_retreating_stat %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(perc_poss_completed = 1- sum(shifted)/n())

write_csv(game_stat, "data/trail_retreating_games.csv")
write_csv(season_stat, "data/trail_retreating_season.csv")
