# Calculate time spent by the 28 foot mark as the trail.
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

source("scripts/helper.R")

trail_mark_stat <-
    track %>%
    filter(teamId == 0, gcStopped == FALSE) %>%
    inner_join(poss, by = c("gameDate", "gameId", "period")) %>%
    filter(
        between(wcTime, wcStart, wcEnd),
        sign(ballX) == sign(basketX)
        # Ball is more than 3 feet past 28 foot mark -- as trail still lags ball
        # abs(ballX) > 22
    )  %>%
    mutate(by_28_mark = if_else(between(abs(x), 16, 22), 1, 0)) %>%
    group_by(gameId, possNum, playerId) %>%
    summarise(by_28_mark = sum(by_28_mark, na.rm = TRUE), n_frames = n()) %>%
    collect() %>%
    group_by(gameId, possNum) %>%
    slice(which.max(sum(by_28_mark)/n_frames))

poss_stat <-
    trail_mark_stat %>%
    group_by(gameId, possNum, playerId) %>%
    summarise(perc_time_by_28_mark = sum(by_28_mark)/sum(n_frames)) %>%
    arrange(gameId, possNum, playerId) %>%
    select(gameId, possNum, playerId, perc_time_by_28_mark)

game_stat <-
    trail_mark_stat %>%
    group_by(gameId, playerId) %>%
    summarise(perc_time_by_28_mark = sum(by_28_mark)/sum(n_frames)) %>%
    arrange(gameId, playerId) %>%
    select(gameId, playerId, perc_time_by_28_mark)

season_stat <-
    trail_mark_stat %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(perc_time_by_28_mark = sum(by_28_mark)/sum(n_frames)) %>%
    arrange(playerId, season) %>%
    select(playerId, season, perc_time_by_28_mark)

write_csv(poss_stat, "data/trail_28_mark_poss.csv")
write_csv(game_stat, "data/trail_28_mark_games.csv")
write_csv(season_stat, "data/trail_28_mark_season.csv")
