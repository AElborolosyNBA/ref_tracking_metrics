# Calculate time spent behind the ball on transition as the trail
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

source("scripts/helper.R")

trailing_transition_query <-
    "
SELECT
    track.gameId,
    poss.possNum,
    track.playerId,
    -- Ref is 3+ feet behind ball.
    SUM(CASE WHEN track.x <= track.ballX - 3 THEN 1 ELSE 0 END) AS trail_behind_ball,
    CAST(COUNT(*) AS NUMERIC) AS n_frames
FROM
    `nba-tracking-data.NbaPlayerTracking.Tracking` track
INNER JOIN
    `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
    track.gameDate = poss.gameDate
    AND track.gameId = poss.gameId
    AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
WHERE
    track.teamId = 0
    AND sign(track.ballX) <> sign(poss.basketX)
    AND NOT gcStopped
GROUP BY
    track.gameId,
    poss.possNum,
    track.playerId
ORDER BY
    track.gameId,
    poss.possNum,
    track.playerId
    "

trailing_transition_stat <-
    dbGetQuery(gbq, trailing_transition_query) %>%
    group_by(gameId, possNum) %>%
    slice(which.max(trail_behind_ball/n_frames))

game_stat <-
    trailing_transition_stat %>%
    group_by(gameId, playerId) %>%
    summarise(
        perc_time_behind_ball_transition = sum(trail_behind_ball)/sum(n_frames)
    ) %>%
    arrange(gameId, playerId) %>%
    select(gameId, playerId, perc_time_behind_ball_transition)

season_stat <-
    trailing_transition_stat %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(
        perc_time_behind_ball_transition = sum(trail_behind_ball)/sum(n_frames)
    ) %>%
    arrange(playerId, season) %>%
    select(playerId, season, perc_time_behind_ball_transition)

write_csv(game_stat, "data/trail_transition_games.csv")
write_csv(season_stat, "data/trail_transition_season.csv")