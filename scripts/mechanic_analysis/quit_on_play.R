# Analysis of trail retreating metric vs GRS Data.
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

trail_retreating_stat <- read_csv("./data/trail_retreating_poss.csv")

relevant_reviews <-
    poss %>%
    select(gameDate, gameId, possNum, wcEnd) %>%
    inner_join(., gbq_reviews) %>%
    # Review occurs in last 5 seconds of play
    filter(respRefLoc == "Trail", wcTime >= wcEnd - 5000) %>%
    mutate(
        callType = case_when(
            (playerAction %in% c("INF", "WPA") & called == "C") ~ "CC",
            (playerAction %in% c("INF") & called == "NC") ~ "INC",
            (playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "C") ~ "IC",
            (playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "NC") ~ "CNC",
            TRUE ~ "None"
        )
    ) %>%
    filter(callType %in% c("CC", "IC", "INC")) %>%
    collect()


joint_data <-
    inner_join(ref_name_map, trail_retreating_stat, by = c("playerId")) %>%
    select(-playerId) %>%
    inner_join(relevant_reviews, by = c("gameId", "possNum")) %>%
    filter(officialId == respRefId) %>%
    mutate(shifted = if_else(shifted == 1, "Quit", "Stayed")) %>%
    count(shifted, callType, name="cnt")

ggplot(
    joint_data,
    aes(fill=callType, y=cnt, x=shifted)
) + 
    geom_bar(position="fill", stat="identity") +
    ggtitle("Possessions Quit On") +
    labs(
        x = 'Possessions Quit',
        y = '% of Events',
        fill = 'Call Type'
    ) +
    scale_fill_manual(
        breaks = c("CC", "IC", "INC"),
        values=c("white", "blue", "red")
    )

ggplot(
    joint_data,
    aes(fill=callType, y=cnt, x=shifted)
) + 
    geom_bar(position="stack", stat="identity") +
    ggtitle("Possessions Quit On") +
    labs(
        x = 'Possessions Quit',
        y = '# of Events',
        fill = 'Call Type'
    ) +
    geom_text_repel(stat='identity', aes(label=cnt)) +
    scale_fill_manual(
        breaks = c("CC", "IC", "INC"),
        values=c("white", "blue", "red")
    )

ref_stats <-
    inner_join(ref_name_map, relevant_reviews, by= c("officialId" = "respRefId")) %>%
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

ref_retreating_stats <-
    inner_join(ref_name_map, trail_retreating_stat, by = c("playerId")) %>%
    select(-playerId) %>%
    inner_join(relevant_reviews, by = c("gameId", "possNum")) %>%
    filter(officialId == respRefId) %>%
    mutate(shifted = if_else(shifted == 1, "Quit", "Stayed")) %>%
    count(Name, shifted, callType) %>%
    spread(callType, n) %>%
    mutate_all(~replace_na(., 0)) %>%
    mutate(
        errors = IC + INC,
        call_accuracy = CC / (CC + IC),
        infraction_accuracy = CC / (CC + INC + IC),
        ic_inc_ratio = IC/INC
    ) %>%
    filter(CC + errors >= 10)

metrics <-
    read_csv("./data/season_aggregate.csv") %>%
    filter(season == "00220")
