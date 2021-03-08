# Analyse trail to lead time vs GRS Errors
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

transition_speed_stat <- readr::read_csv("./data/lead_transition_poss.csv")

relevant_reviews <-
    dbGetQuery(
        gbq,
        "
WITH transition_end AS (
    SELECT
        track.gameDate,
        track.gameId,
        poss.possNum,
        track.playerId,
        min(track.wcTime) AS transition_end
    FROM
        `nba-tracking-data.NbaPlayerTracking.Tracking` track
    INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
        track.gameDate = poss.gameDate
        AND track.gameId = poss.gameId
        AND track.period = poss.period
        AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
    WHERE
        track.teamId = 0
        AND sign(track.x) = sign(poss.basketX)
        AND abs(track.x) > abs(poss.basketX)
    GROUP BY
        track.gameDate,
        track.gameId,
        poss.possNum,
        track.playerId
)
SELECT
    grs.gameId,
    grs.possNum,
    grs.eventId,
    grs.respRefId,
    grs.playerAction,
    grs.called
FROM
    `bball-strategy-analytics.GrsReviews.Reviews` grs
INNER JOIN transition_end ON
    grs.wcTime <= transition_end.transition_end
    AND transition_end.gameDate = grs.gameDate
    AND transition_end.gameId = grs.gameId
    AND transition_end.possNum = grs.possNum
WHERE
    grs.respRefLoc = 'Lead'
    "
    ) %>%
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
    distinct()
    
joint_data <-
    inner_join(transition_speed_stat, relevant_reviews) %>%
    inner_join(., ref_name_map) %>%
    filter(officialId == respRefId) %>%
    ungroup() %>%
    mutate(transition_speed = round(transition_speed)) %>%
    count(transition_speed, callType, name="cnt") %>%
    filter(callType %in% c("CC", "IC", "INC"), transition_speed <= 20)

ggplot(
    joint_data,
    aes(fill=callType, y=cnt, x=transition_speed)
) + 
    geom_bar(position="fill", stat="identity") +
    ggtitle("Trail to Lead - Speed vs Errors") +
    labs(
        x = 'Transition Speed (Miles Per Hour)',
        y = '% of Events',
        fill = 'Call Type'
    ) +
    scale_fill_manual(
        breaks = c("CC", "INC", "IC"),
        values=c("white", "blue", "red")
        )

ggplot(
    joint_data,
    aes(fill=callType, y=cnt, x=transition_speed)
) + 
    geom_bar(position="stack", stat="identity") +
    ggtitle("Trail to Lead - Speed vs Errors") +
    xlab("Transition Speed (Miles Per Hour)") +
    ylab("Number of Events") +
    geom_text_repel(stat='identity', aes(label=cnt))

ref_stats <-
    inner_join(transition_speed_stat, relevant_reviews) %>%
    inner_join(., ref_name_map, by = c("respRefId" = "officialId")) %>%
    filter(grepl("00220*", gameId)) %>%
    group_by(Name) %>% 
    count(callType) %>%
    spread(callType, n) %>%
    mutate_at(., vars(-group_cols()), ~replace_na(., 0)) %>%
    mutate(
        errors = IC + INC,
        call_accuracy = CC / (CC + IC),
        infraction_accuracy = CC / (CC + INC + IC)
    )

staff_stats <-
    inner_join(transition_speed_stat, relevant_reviews) %>%
    filter(grepl("00220*", gameId)) %>%
    count(callType) %>%
    spread(callType, n) %>%
    mutate(
        errors = IC + INC,
        ic_inc_ratio = IC/INC,
        call_accuracy = CC / (CC + IC),
        infraction_accuracy = CC / (CC + INC + IC)
    )

metrics <-
    read_csv("./data/season_aggregate.csv") %>%
    filter(season == "00220")
