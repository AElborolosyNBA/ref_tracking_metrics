# Calculate time it takes lead to reach the baseline.
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

source("scripts/helper.R")

transition_speed_query <- c(
    "
WITH transition_end AS (
SELECT
    track.gameDate,
    track.gameId,
    poss.possNum,
    track.playerId,
    min(track.wcTime) AS transition_end
FROM
    `nba-tracking-data.NbaPlayerTracking.Tracking` track
INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
    track.gameDate = poss.gameDate
    AND track.gameId = poss.gameId
    AND track.period = poss.period
    AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
WHERE
    track.teamId = 0
    AND sign(track.x) = sign(poss.basketX)
    AND abs(track.x) > abs(poss.basketX)
GROUP BY
    track.gameDate,
    track.gameId,
    poss.possNum,
    track.playerId
    )
SELECT
    gameId,
    possNum,
    playerId,
    transition_distance,
    CASE
        WHEN transition_time <> 0 THEN transition_distance / transition_time
        ELSE NULL
    END AS transition_speed
FROM
(
    SELECT
        track.gameId,
        poss.possNum,
        track.playerId,
        (transition_end.transition_end - poss.wcStart)/ 1000 AS transition_time,
        CAST(abs(track.x - poss.basketX) AS NUMERIC) AS transition_distance
    FROM
        `nba-tracking-data.NbaPlayerTracking.Tracking` track
    INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
        track.gameDate = poss.gameDate
        AND track.gameId = poss.gameId
        AND track.period = poss.period
        AND track.wcTime = poss.wcStart
    INNER JOIN transition_end ON
        transition_end.gameDate = track.gameDate
        AND transition_end.gameId = track.gameId
        AND transition_end.possNum = poss.possNum
        AND transition_end.playerId = track.playerId
    WHERE
        track.teamId = 0
        AND NOT gcStopped)
    "
)

transition_speed_stat <-
    DBI::dbGetQuery(gbq, transition_speed_query) %>%
    group_by(gameId, possNum) %>%
    filter(!is.na(transition_speed)) %>%
    slice(which.min(transition_distance)) %>%
    # Convert feet per second to miles per hour
    mutate(transition_speed = 0.681818 * transition_speed) %>%
    ungroup() %>%
    select(gameId, possNum, playerId, transition_speed)

game_stat <- 
    transition_speed_stat %>%
    group_by(gameId, playerId) %>%
    summarise(transition_speed = mean(transition_speed, na.rm=TRUE))

season_stat <-
    transition_speed_stat %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(transition_speed = mean(transition_speed, na.rm=TRUE))

write_csv(transition_speed_stat, "data/lead_transition_poss.csv")
write_csv(game_stat, "data/lead_transition_games.csv")
write_csv(season_stat, "data/lead_transition_season.csv")
