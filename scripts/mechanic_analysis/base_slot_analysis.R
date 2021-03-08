# Analyse slot by FT Externded vs GRS Errors
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
    filter(respRefLoc == 'Slot', callType %in% c("CC", "IC", "INC")) %>%
    inner_join(track, by = c("gameDate", "gameId", "wcTime")) %>%
    filter(teamId == 0) %>%
    select(
        gameDate, gameId, possNum, playerId, respRefId, callType, x, ballX
    ) %>%
    inner_join(poss, by = c("gameDate", "gameId", "possNum")) %>%
    filter(sign(basketX) == sign(ballX)) %>%
    mutate(
        slot_by_ft_ext = case_when(
            between(abs(x), 25, 31) ~ "FT Line",
            abs(x) < 25 ~ "Above",
            abs(x) > 25 ~ "Below",
            TRUE ~ "Error"
        )
    ) %>%
    select(
        gameDate, gameId, possNum, playerId, officialId = respRefId,
        callType, slot_by_ft_ext
    ) %>%
    collect() %>%
    inner_join(., ref_name_map, by = c("playerId", "officialId"))

ggplot(
    relevant_calls %>% count(slot_by_ft_ext, callType, name = "cnt"),
    aes(fill=callType, y=cnt, x=slot_by_ft_ext)
) + 
    geom_bar(position="fill", stat="identity") +
    coord_cartesian(ylim=c(0, 0.5)) + 
    ggtitle("Slot Halfcourt - Position vs Errors") +
    labs(
        x = 'Above/Below/At FT Line Extended',
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
    count(Name, playerId, callType) %>%
    spread(callType, n) %>%
    mutate_all(~replace_na(., 0)) %>%
    mutate(
        errors = IC + INC,
        call_accuracy = CC / (CC + IC),
        infraction_accuracy = CC / (CC + INC + IC),
        ic_inc_ratio = IC/INC
    ) %>%
    filter(CC + errors >= 10)

metric_performance <-
    read_csv("./data/slot_ft_line_season.csv") %>%
    filter(season == "00220")

mechanics_effectiveness <- inner_join(ref_stats, metric_performance)

ggplot(
    data = mechanics_effectiveness,
    aes(
        x=perc_time_by_ft_line_ext,
        y=infraction_accuracy
    )
) +
    geom_point() +
    labs(
        title = "Slot by FT line - Consistency vs Accuracy",
        subtitle = "Outlier Detection -- Spot Underperforming Refs",
        x = 'By FT Line Ext (<3 Feet Away from FT Line Ext',
        y = 'Infraction Accuracy'
    )
