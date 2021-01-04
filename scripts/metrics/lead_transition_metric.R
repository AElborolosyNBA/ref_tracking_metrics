# Calculate time it takes lead to reach the baseline.
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
	AND sign(track.x) = sign(poss.basketX)
	AND abs(track.x) > abs(poss.basketX)
	AND poss.wcEnd - poss.wcStart >= 3000 -- Possessions longer than 3 seconds
GROUP BY
    track.gameDate,
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

transition_time <-
    dbGetQuery(gbq, lead_transition_stat) %>% inner_join(refs_by_poss)
    
game_stat <- 
    transition_time %>%
    group_by(gameId, playerId) %>%
    summarise(time_to_baseline = mean(transition_time, na.rm=TRUE))

season_stat <-
    transition_time %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(time_to_baseline = mean(transition_time, na.rm=TRUE))

write_csv(game_stat, "data/lead_transition_games.csv")
write_csv(season_stat, "data/lead_transition_season.csv")
