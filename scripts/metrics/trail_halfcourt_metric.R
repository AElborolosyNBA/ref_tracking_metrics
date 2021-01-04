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
    
is_trailing <-
    track %>%
    filter(gameDate >= '2019-10-22', teamId == 0, gcStopped == FALSE) %>%
    inner_join(poss, by = c("gameId", "gameDate", "period")) %>%
    filter(
        between(wcTime, wcStart, wcEnd),
        # Ball  crossed halfcourt
        sign(ballX) == sign(basketX),
        # Referee is behind basketball
        (x < ballX && basketX > 0) || (x > ballX && basketX < 0)
    ) %>%
    group_by(gameDate, gameId, possNum, playerId) %>%
    summarise(time_trailing = n()/25) %>%
    arrange(gameDate, gameId, possNum, playerId) %>%
    collect()

trail_transition_stat <-
    left_join(transition_time, is_trailing) %>%
    mutate(time_trailing = replace_na(time_trailing, 0)) %>%
    inner_join(
        refs_by_poss,
        by = c("gameDate", "gameId", "possNum", "playerId")
    )

game_stat <-
    trail_transition_stat %>%
    group_by(gameId, playerId) %>%
    summarise(
        transition_time = sum(transition_time),
        time_trailing = sum(time_trailing),
        poss = n()
    ) %>%
    mutate(
        perc_time_in_position_halfcourt = 100 * time_trailing/transition_time
    ) %>%
    arrange(gameId, playerId) %>%
    select(gameId, playerId, perc_time_in_position_halfcourt)

season_stat <-
    trail_transition_stat %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(
        transition_time = sum(transition_time),
        time_trailing = sum(time_trailing),
        poss = n()
    ) %>%
    mutate(
        perc_time_in_position_halfcourt = 100 * time_trailing/transition_time
    ) %>%
    select(season, playerId, perc_time_in_position_halfcourt)

write_csv(game_stat, "data/trail_halfcourt_games.csv")
write_csv(season_stat, "data/trail_halfcourt_season.csv")
