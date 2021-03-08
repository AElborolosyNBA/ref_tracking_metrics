# Analyse lead being wide vs GRS Errors
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
    filter(respRefLoc == 'Lead', callType %in% c("CC", "IC", "INC")) %>%
    inner_join(track, by = c("gameDate", "gameId", "wcTime")) %>%
    filter(teamId == 0) %>%
    select(
        gameDate, gameId, possNum, playerId, respRefId, callType, y, ballX
    ) %>%
    inner_join(poss, by = c("gameDate", "gameId", "possNum")) %>%
    filter(sign(basketX) == sign(ballX)) %>%
    mutate(wide_lead = if_else(abs(y) >= 12, 1, 0)) %>%
    select(
        gameDate, gameId, possNum, playerId, officialId = respRefId,
        callType, wide_lead
    ) %>%
    collect() %>%
    inner_join(., ref_name_map, by = c("playerId", "officialId"))

ggplot(
    relevant_calls %>% count(wide_lead, callType, name = "cnt"),
    aes(fill=callType, y=cnt, x=as.integer(wide_lead))
) + 
    geom_bar(position="fill", stat="identity") +
    coord_cartesian(ylim=c(0, 0.5)) + 
    ggtitle("Lead Halfcourt - Position vs Errors") +
    labs(
        x = 'Wide or Not?',
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
    read_csv("./data/wide_lead_season.csv") %>%
    filter(season == "00220")

mechanics_effectiveness <- inner_join(ref_stats, metric_performance)

ggplot(
    data = mechanics_effectiveness,
    aes(
        x=perc_time_in_base_position_lead,
        y=infraction_accuracy
    )
) +
    geom_point() +
    labs(
        title = "Lead is Wide - Consistency vs Accuracy",
        subtitle = "Outlier Detection -- Spot Underperforming Refs",
        x = 'Time Wide (12+ Feet Away from Rim)',
        y = 'Infraction Accuracy'
    )
