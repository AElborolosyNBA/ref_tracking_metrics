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

refs_by_poss <-
    identify_ref_position(gbq) %>%
    filter(playerType == 'Trail')

transition_time <-
    track %>%
    filter(gameDate >= '2019-10-22', teamId == -1, gcStopped == FALSE) %>%
    inner_join(poss, by = c("gameId", "gameDate", "period")) %>%
    filter(between(wcTime, wcStart, wcEnd), sign(ballX) == sign(basketX)) %>%
    group_by(gameDate, gameId, possNum) %>%
    summarise(transition_time = n()/25) %>%
    arrange(gameDate, gameId, possNum) %>%
    collect()

time_by_mark <-
    track %>%
    filter(gameDate >= '2019-10-22', teamId == 0, gcStopped == FALSE) %>%
    inner_join(poss, by = c("gameId", "gameDate", "period")) %>%
    filter(
        between(wcTime, wcStart, wcEnd),
        # Ball and Ref crossed halfcourt
        sign(ballX) == sign(basketX),
        sign(x) == sign(basketX),
        # Referee is within 3 feet of 28 foot mark
        between(abs(x), 16, 22)
    ) %>%
    group_by(gameDate, gameId, possNum, playerId) %>%
    summarise(time_positioned = n()/25) %>%
    arrange(gameDate, gameId, possNum, playerId) %>%
    collect()

trail_mark_stat <-
    left_join(transition_time, time_by_mark) %>%
    mutate(time_positioned = replace_na(time_positioned, 0)) %>%
    inner_join(
        refs_by_poss,
        by = c("gameDate", "gameId", "possNum", "playerId")
    )

game_stat <-
    trail_mark_stat %>%
    group_by(gameId, playerId) %>%
    summarise(
        transition_time = sum(transition_time),
        time_positioned = sum(time_positioned),
        poss = n()
    ) %>%
    mutate(
        perc_time_in_base_position_trail = 100 * time_positioned/transition_time
    ) %>%
    arrange(gameId, playerId) %>%
    select(gameId, playerId, perc_time_in_base_position_trail)

season_stat <-
    trail_mark_stat %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(
        transition_time = sum(transition_time),
        time_positioned = sum(time_positioned),
        poss = n()
    ) %>%
    mutate(
        perc_time_in_base_position_trail = 100 * time_positioned/transition_time
    ) %>%
    arrange(playerId, season) %>%
    select(playerId, season, perc_time_in_base_position_trail)


write_csv(game_stat, "data/trail_28_mark_games.csv")
write_csv(season_stat, "data/trail_28_mark_season.csv")
