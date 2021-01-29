# Calculate distance between ref and median of players
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

source("scripts/helper.R")

refs_by_poss <-
    identify_ref_position(gbq) %>%
    filter(playerType == 'Slot')

slot_distance_stat <-
    c(
    "
WITH middle_of_pack AS (
SELECT
	track.gameDate,
	track.gameId,
	track.wcTime,
	PERCENTILE_CONT(track.x, 0.5) OVER(PARTITION BY track.gameDate,
	track.gameId,
	track.wcTime) AS middle_of_pack
FROM
	`nba-tracking-data.NbaPlayerTracking.Tracking` track
WHERE
	track.gameDate >= '2019-10-22'
	AND track.teamId NOT IN (-1, 0)
)
SELECT
    track.gameDate,
    track.gameId,
    poss.possNum,
    track.playerId,
    AVG(ABS(track.x - middle_of_pack.middle_of_pack)) AS avg_dist_from_pack
FROM
    `nba-tracking-data.NbaPlayerTracking.Tracking` track
INNER JOIN
    middle_of_pack ON 
    middle_of_pack.gameDate = track.gameDate
    AND middle_of_pack.gameId = track.gameId
    AND middle_of_pack.wcTime = track.wcTime
INNER JOIN
    `nba-tracking-data.NbaPlayerTracking.Possessions` poss
    ON poss.gameDate = track.gameDate
    AND poss.gameId = track.gameId
    AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
WHERE
    track.teamId = 0
    AND sign(track.ballX) <> sign(poss.basketX)
    AND track.gcStopped IS FALSE
GROUP BY
    track.gameDate,
    track.gameId,
    poss.possNum,
    track.playerId;
    "
)

slot_distance <-
    dbGetQuery(gbq, slot_distance_stat) %>% inner_join(refs_by_poss)

game_stat <- 
    slot_distance %>%
    group_by(gameId, playerId) %>%
    summarise(distance_from_pack = mean(avg_dist_from_pack, na.rm=TRUE))

season_stat <-
    slot_distance %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(distance_from_pack = mean(avg_dist_from_pack, na.rm=TRUE))

write_csv(game_stat, "data/slot_transition_games.csv")
write_csv(season_stat, "data/slot_transition_season.csv")