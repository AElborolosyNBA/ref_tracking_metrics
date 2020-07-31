# Calculate distance between ref and median of players
library(checkpoint)
checkpoint("2019-12-30")

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
SELECT
	ref_loc.gameDate,
	ref_loc.gameId,
	poss.possNum,
	ref_loc.playerId,
	AVG(ref_loc.x - player_loc.middle_of_pack) AS raw_dist,
	AVG(ABS(ref_loc.x - player_loc.middle_of_pack)) AS abs_dist
FROM
	(
	SELECT
		track.gameDate,
		track.gameId,
		track.wcTime,
		track.playerId,
		x
	FROM
		`nba-tracking-data.NbaPlayerTracking.Tracking` track
	WHERE
		track.gameDate >= '2019-10-22'
		AND track.teamId = 0) AS ref_loc
INNER JOIN (
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
		AND track.teamId NOT IN (-1,
		0)) player_loc ON
	player_loc.gameDate = ref_loc.gameDate
	AND player_loc.gameId = ref_loc.gameId
	AND player_loc.wcTime = ref_loc.wcTime
INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
	poss.gameDate = ref_loc.gameDate
	AND poss.gameId = ref_loc.gameId
	AND ref_loc.wcTime BETWEEN poss.wcStart AND poss.wcEnd
GROUP BY
	ref_loc.gameDate,
	ref_loc.gameId,
	poss.possNum,
	ref_loc.playerId
ORDER BY
	ref_loc.gameDate,
	ref_loc.gameId,
	poss.possNum,
	ref_loc.playerId
"
    )

slot_distance <- dbGetQuery(gbq, slot_distance_stat)

game_stat <- 
    slot_distance %>%
    inner_join(refs_by_poss) %>%
    group_by(gameId, playerId) %>%
    summarise(distance_from_pack = mean(abs_dist, na.rm=TRUE))

season_stat <-
    slot_distance %>%
    inner_join(refs_by_poss) %>%
    group_by(playerId) %>%
    summarise(distance_from_pack = mean(abs_dist, na.rm=TRUE))

write_csv(game_stat, "data/slot_transition_games.csv")
write_csv(season_stat, "data/slot_transition_season.csv")