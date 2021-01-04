# Calculate the distance the trail 'retrats' at the end of a possession.
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

event <- tbl(gbq, "nba-tracking-data.NbaPlayerTracking.Events")

ordered_events <-
    event %>%
    filter(!(eventType %in% c("FT", "REP", "TMO", "JMP"))) %>%
    select(gameDate, gameId, period, possId, gcTime, wcTime) %>%
    group_by(gameDate, gameId, period, possId) %>%
    arrange(desc(gcTime)) %>%
    mutate(event_order = row_number()) %>%
    ungroup() %>%
    filter(event_order == 1) %>%
    select(
        gameDate, gameId, period, possId,
        timer_start = gcTime, dupe_check = wcTime,
        -event_order
    )

ref_loc <-
    track %>%
    filter(teamId == 0, gcStopped == FALSE) %>%
    inner_join(poss, by=c("gameDate", "gameId", "period")) %>%
    filter(between(gcTime, gcEnd, gcStart)) %>%
    select(
        gameDate, gameId, period, possId, basketX, wcTime,
        gcTime, x, y, playerId, gcStart, gcEnd, wcEnd, possNum
    ) %>%
    inner_join(
        ordered_events,
        by = c("gameDate", "gameId", "period", "possId")
    ) %>%
    filter(wcTime == dupe_check || wcTime == wcEnd) %>%
    mutate(
        position = ifelse(
            gcTime == timer_start,
            "Start",
            ifelse(gcTime == gcEnd, "End", "Error")
        )
    )

start <-
    filter(ref_loc, position == "Start") %>%
    select(
        gameDate, gameId, period, possNum, playerId, basketX,
        start_x = x, start_y = y, start_clock = gcTime
    ) %>%
    mutate(start_dist = abs(start_x - basketX))

end <-
    filter(ref_loc, position == "End") %>%
    select(
        gameDate, gameId, period, possNum, playerId, basketX,
        end_x = x, end_y = y, end_clock = gcTime
    ) %>%
    mutate(end_dist = abs(end_x - basketX))

res <-
    inner_join(start, end) %>%
    mutate(
        shift = end_dist - start_dist,
        t = start_clock - end_clock, # Using decrementing gcTime
    ) %>%
    filter(t > 0) %>%
    select(gameDate, gameId, period, possNum, playerId, shift, t) %>%
    collect() %>%
    mutate(v_shift = shift/t)

game_stat <-
    res %>%
    group_by(gameId, playerId) %>%
    summarise(
        n = n(),
        shifted = sum(ifelse(shift > 3.0, 1, 0)),
        d_shift = sum(ifelse(shift > 3.0, shift, 0))
    ) %>%
    mutate(
        shift_perc = 100*shifted/n,
        avg_d_shft = d_shift/shifted
    ) %>%
    select(
        gameId,
        playerId,
        perc_poss_quit = shift_perc
    )

season_stat <-
    res %>%
    mutate(season = substr(gameId, 1, 5)) %>%
    group_by(season, playerId) %>%
    summarise(
        n = n(),
        shifted = sum(ifelse(shift > 3.0, 1, 0)),
        d_shift = sum(ifelse(shift > 3.0, shift, 0))
    ) %>%
    mutate(
        shift_perc = 100*shifted/n,
        avg_d_shft = d_shift/shifted
    ) %>%
    select(
        season, playerId,
        perc_poss_quit = shift_perc
    )

write_csv(game_stat, "data/trail_retreating_games.csv")
write_csv(season_stat, "data/trail_retreating_season.csv")
