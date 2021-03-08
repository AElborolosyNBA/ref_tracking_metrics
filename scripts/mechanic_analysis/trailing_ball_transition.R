# Analyse trail behind ball in transition vs GRS Errors
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(readr)
library(tidyr)

source("scripts/helper.R")

ref_name_map <-
    ref_jerseys %>%
    select(Name, playerId = jerseyNum, officialId) %>%
    distinct() %>%
    collect()

relevant_calls <-
    gbq_reviews %>%
    select(
        gameDate, gameId, possNum, eventId, wcTime,
        respRefId, respRefLoc, playerAction, called
    ) %>%
    distinct() %>%
    mutate(
        callType = case_when(
            (playerAction %in% c("INF", "WPA") & called == "C") ~ "CC",
            (playerAction %in% c("INF") & called == "NC") ~ "INC",
            (playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "C") ~ "IC",
            (playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "NC") ~ "CNC",
            TRUE ~ "None"
        )
    ) %>%
    filter(respRefLoc == 'Trail', callType %in% c("CC", "IC", "INC")) %>%
    inner_join(track, by = c("gameDate", "gameId", "wcTime")) %>%
    filter(teamId == 0) %>%
    select(
        gameDate, gameId, possNum, playerId, respRefId, callType, x, ballX
    ) %>%
    inner_join(poss, by = c("gameDate", "gameId", "possNum")) %>%
    filter(sign(basketX) != sign(ballX)) %>%
    mutate(
        trailing = case_when(
            basketX < 0 & x <= ballX + 3 ~ "Yes",
            basketX > 0 & x <= ballX - 3 ~ "Yes",
            basketX < 0 & x > ballX + 3 ~ "No",
            basketX > 0 & x > ballX - 3 ~ "No",
            TRUE ~ "Error"
        )
    ) %>%
    select(
        gameDate, gameId, possNum, playerId, officialId = respRefId,
        callType, trailing
    ) %>%
    collect() %>%
    inner_join(., ref_name_map, by = c("playerId", "officialId"))

ggplot(
    relevant_calls %>% count(trailing, callType, name = "cnt"),
    aes(fill=callType, y=cnt, x=trailing)
) + 
    geom_bar(position="fill", stat="identity") +
    ggtitle("Trail Transition - Position vs Errors") +
    labs(
        x = 'Trailing Ball or Not?',
        y = '% of Events',
        fill = 'Call Type'
    ) +
    scale_fill_manual(
        breaks = c("CC", "INC", "IC"),
        values=c("white", "blue", "red")
    )

ref_stats <-
    inner_join(ref_name_map, relevant_calls) %>%
    filter(grepl("00220*", gameId)) %>%
    count(Name, callType) %>%
    spread(callType, n) %>%
    mutate_all(~replace_na(., 0)) %>%
    mutate(
        errors = IC + INC,
        call_accuracy = CC / (CC + IC),
        infraction_accuracy = CC / (CC + INC + IC),
        total_accuracy = (CC + CNC) / (CC + CNC + INC + IC),
        ic_inc_ratio = IC/INC
    ) %>%
    filter(CC + errors >= 10)

all_metrics <-
    read_csv("./data/season_aggregate.csv") %>%
    filter(season == "00220")

mechanics_effectiveness <- inner_join(ref_stats, all_metrics)

ggplot(
    data = mechanics_effectiveness,
    aes(
        x=perc_time_behind_ball_transition,
        y=infraction_accuracy,
        fill=as.numeric(mechanics_effectiveness$errors)
    )
) +
    geom_point() +
    labs(
        title = "Trailing the Ball - Consistency vs Accuracy",
        subtitle = "R2 of 0.13: Trailing the ball assists w/ play calling",
        x = 'Time Trailing Ball (%)',
        y = 'Infraction Accuracy (%)',
        fill = 'Errors'
    ) 
