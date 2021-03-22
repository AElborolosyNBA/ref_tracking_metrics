# Calculate time slot spend within 3 feet of FT line extended.
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

source("scripts/helper.R")

ft_line_query <-
    "
SELECT
    track.gameId,
    poss.possNum,
    track.playerId,
	SUM(CASE WHEN abs(track.x) BETWEEN 25 AND 31 THEN 1 ELSE 0 END) AS by_ft_line,
    COUNT(*) AS n_frames
FROM
    `nba-tracking-data.NbaPlayerTracking.Tracking` track
INNER JOIN
    `nba-tracking-data.NbaPlayerTracking.Possessions` poss
    ON poss.gameDate = track.gameDate
    AND poss.gameId = track.gameId
    AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
WHERE
    track.teamId = 0
    AND gcStopped IS FALSE
    AND sign(track.ballX) = sign(poss.basketX)
GROUP BY
    track.gameId,
    poss.possNum,
    track.playerId
ORDER BY
    track.gameId,
    poss.possNum,
    track.playerId
    "

ft_line_stats <-
    dbGetQuery(gbq, ft_line_query) %>%
    group_by(gameId, possNum) %>%
    slice(which.max(by_ft_line/n_frames))

poss_stat <-
    ft_line_stats %>%
    group_by(gameId, possNum, playerId) %>%
    summarise(perc_time_by_ft_line_ext = sum(by_ft_line)/sum(n_frames)) %>%
    select(gameId, possNum, playerId, perc_time_by_ft_line_ext)

game_stat <- 
    ft_line_stats %>%
    group_by(gameId, playerId) %>%
    summarise(perc_time_by_ft_line_ext = sum(by_ft_line)/sum(n_frames)) %>%
    select(gameId, playerId, perc_time_by_ft_line_ext)

season_stat <-
    ft_line_stats %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(perc_time_by_ft_line_ext = sum(by_ft_line)/sum(n_frames)) %>%
    select(season, playerId, perc_time_by_ft_line_ext)

write_csv(poss_stat, "data/slot_ft_line_poss.csv")
write_csv(game_stat, "data/slot_ft_line_games.csv")
write_csv(season_stat, "data/slot_ft_line_season.csv")
