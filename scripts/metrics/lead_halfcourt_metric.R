# Calculate time spent at least 12 feet away from rim on the baseline.
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

source("scripts/helper.R")

baseline_query <-
    "
    SELECT
        track.gameId,
        poss.possNum,
        track.playerId,
        SUM(CASE WHEN track.y >= 12 THEN 1 ELSE 0 END) AS wide_lead,
        CAST(COUNT(*) AS NUMERIC) AS n_frames
    FROM
        `nba-tracking-data.NbaPlayerTracking.Tracking` track
    INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
        track.gameDate = poss.gameDate
        AND track.gameId = poss.gameId
        AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
    WHERE
        --track.gameDate = '2019-10-22' AND
        track.teamId = 0
        AND sign(track.x) = sign(poss.basketX)
        AND abs(track.x) >= 43
        -- Ref is behind rim.
    GROUP BY
        track.gameId,
        poss.possNum,
        track.playerId
    ORDER BY
        track.gameId,
        poss.possNum,
        track.playerId
    "

baseline_stat <-
    dbGetQuery(gbq, baseline_query) %>%
    group_by(gameId, possNum) %>%
    slice(which.max(wide_lead/n_frames))

game_stat <-
    baseline_stat %>%
    group_by(gameId, playerId) %>%
    summarise(
        wide_lead = sum(wide_lead),
        n_frames = sum(n_frames)
    ) %>%
    mutate(
        perc_time_in_base_position_lead = wide_lead/n_frames
    ) %>%
    arrange(gameId, playerId) %>%
    select(gameId, playerId, perc_time_in_base_position_lead)

season_stat <-
    baseline_stat %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(
        wide_lead = sum(wide_lead),
        n_frames = sum(n_frames)
    ) %>%
    mutate(
        perc_time_in_base_position_lead = wide_lead/n_frames
    ) %>%
    arrange(playerId, season) %>%
    select(playerId, season, perc_time_in_base_position_lead)

write_csv(game_stat, "data/wide_lead_games.csv")
write_csv(season_stat, "data/wide_lead_season.csv")