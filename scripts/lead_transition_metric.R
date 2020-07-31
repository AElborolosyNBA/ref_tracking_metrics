# Calculate distance between
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
    filter(playerType == 'Lead')

lead_transition_stat <-
    c(
        "
SELECT
	track.gameDate,
	track.gameId,
	poss.possNum,
	track.playerId,
	(min(track.wcTime) - poss.wcStart)/ 1000 AS transition_time,
FROM
	`nba-tracking-data.NbaPlayerTracking.Tracking` track
INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
	track.gameDate = poss.gameDate
	AND track.gameId = poss.gameId
	AND track.period = poss.period
	AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
WHERE
	track.gameDate >= '2019-10-22'
	AND track.teamId = 0
	AND ((track.x >= 47
	AND poss.basketX > 0)
	OR (track.x <= -47
	AND poss.basketX < 0))
	AND poss.wcEnd - poss.wcStart >= 3000
	-- Possessions longer than 3 seconds

	GROUP BY track.gameDate,
	track.gameId,
	poss.possNum,
	track.playerId,
	poss.wcStart,
	poss.wcEnd
ORDER BY
	track.gameDate,
	track.gameId,
	poss.possNum,
	track.playerId
    "
    )

transition_time <- dbGetQuery(gbq, lead_transition_stat)

game_stat <- 
    transition_time %>%
    inner_join(refs_by_poss) %>%
    group_by(gameId, playerId) %>%
    summarise(time_to_baseline = mean(transition_time, na.rm=TRUE))

season_stat <-
    transition_time %>%
    inner_join(refs_by_poss) %>%
    group_by(playerId) %>%
    summarise(time_to_baseline = mean(transition_time, na.rm=TRUE))

write_csv(game_stat, "data/lead_transition_games.csv")
write_csv(season_stat, "data/lead_transition_season.csv")
