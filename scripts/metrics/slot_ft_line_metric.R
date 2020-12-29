# Calculate time slot spend within 3 feet of FT line extended.
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

poss_duration_query <-
    "
SELECT
    track.gameId,
    poss.possNum,
    (MAX(track.gcTime) - poss.gcEnd) AS halfcourt_poss_duration
FROM
    `nba-tracking-data.NbaPlayerTracking.Tracking` track
INNER JOIN
    `nba-tracking-data.NbaPlayerTracking.Possessions` poss
    ON poss.gameDate = track.gameDate
    AND poss.gameId = track.gameId
    AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
WHERE
    track.teamId = -1
    AND sign(track.ballX) = sign(poss.basketX)
    AND gcTime > gcEnd -- Prevent possessions w/ non-positive durations
GROUP BY
    track.gameId,
    poss.possNum,
    track.playerId,
    poss.gcEnd
    "

ft_line_query <-
    "
SELECT
    track.gameId,
    poss.possNum,
    track.playerId,
    COUNT(*)/25 AS time_by_ft_line_ext
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
	AND abs(track.x) BETWEEN 47 - (19 + 3) AND 47 - (19 - 3)
    AND sign(track.x) = sign(poss.basketX)
GROUP BY
    track.gameId,
    poss.possNum,
    track.playerId
ORDER BY
    track.gameId,
    poss.possNum,
    track.playerId
    "

poss_duration <- dbGetQuery(gbq, poss_duration_query)

ft_line_stats <-
    dbGetQuery(gbq, ft_line_query) %>%
    inner_join(poss_duration) %>%
    inner_join(refs_by_poss) %>%
    # Remove rounding errors in cameras that create 102% stats
    mutate(
        time_by_ft_line_ext = if_else(
            time_by_ft_line_ext > halfcourt_poss_duration,
            halfcourt_poss_duration,
            time_by_ft_line_ext
        )
    )

game_stat <- 
    ft_line_stats %>%
    group_by(gameId, playerId) %>%
    summarise(
        time_by_ft_line_ext = sum(time_by_ft_line_ext),
        halfcourt_poss_duration = sum(halfcourt_poss_duration)
    ) %>%
    mutate(
        perc_time_by_ft_line_ext = time_by_ft_line_ext/halfcourt_poss_duration
    ) %>%
    select(gameId, playerId, perc_time_by_ft_line_ext)

season_stat <-
    ft_line_stats %>%
    group_by(playerId) %>%
    summarise(
        time_by_ft_line_ext = sum(time_by_ft_line_ext),
        halfcourt_poss_duration = sum(halfcourt_poss_duration)
    ) %>%
    mutate(
        perc_time_by_ft_line_ext = time_by_ft_line_ext/halfcourt_poss_duration
    ) %>%
    select(playerId, perc_time_by_ft_line_ext)

write_csv(game_stat, "data/slot_ft_line_game.csv")
write_csv(season_stat, "data/slot_ft_line_season.csv")
