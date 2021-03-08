# Analyse trail behind ball in the halfcourt vs GRS Errors
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(dbplyr)
library(DBI)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(readr)
library(tidyr)
library(bigrquery)

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
    filter(sign(basketX) == sign(ballX)) %>%
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
        ic_inc_ratio = IC/INC
    ) %>%
    filter(CC + errors >= 10)

call_totals <-
    gbq_reviews %>%
    filter(gameId %LIKE% '00220%') %>%
    select(
        gameDate, gameId, possNum, eventId, wcTime, respRefLoc, playerAction, called
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
    count(respRefLoc, callType) %>%
    arrange(respRefLoc, callType) %>%
    collect()
