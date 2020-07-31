# Calculate distance of the lead Ref from the basket during shooting fouls.
library(checkpoint)
checkpoint("2019-12-30")

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(tidyr)

gbq <- dbConnect(
    bigrquery::bigquery(),
    project = "bball-strategy-analytics",
    dataset = "nba-tracking-data",
    billing = "bball-strategy-analytics"
)

track <- tbl(gbq, "nba-tracking-data.NbaPlayerTracking.Tracking")
poss <- tbl(gbq, "nba-tracking-data.NbaPlayerTracking.Possessions")
gbq_reviews <- tbl(gbq, "bball-strategy-analytics.GrsReviews.Reviews")
ref_jerseys <- tbl(gbq, "bball-strategy-analytics.GrsReviews.JerseyLookUp")

# Distance from Ref to Rim ----
shooting_fouls <-
    gbq_reviews %>%
    filter(
        respRefLoc == "Lead",
        infractionType == "Foul: Shooting",
        gameDate >= '2019-10-22'
    ) %>%
    mutate(
        gameDate = as.Date(gameDate),
        callRating = case_when(
            playerAction %in% c("INF", "WPA") & called == "C" ~ "CC",
            playerAction %in% c("INF") & called == "NC" ~ "INC",
            playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "C" ~ "IC",
            playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "NC" ~ "CNC"
        )
    ) %>%
    select(-gcTime) %>%
    inner_join(., track, by = c("gameDate", "gameId", "wcTime", "period")) %>%
    inner_join(., poss, by = c("gameDate", "gameId", "period", "possNum")) %>%
    filter(between(wcTime, wcStart, wcEnd), gcStopped == FALSE) %>%
    select(
        gameDate, gameId, possNum, respRefId,
        x, y, basketX, callRating
    ) %>%
    mutate(distance = sqrt((x - basketX)**2 + y**2)) %>%
    filter(distance <= 25) %>%
    inner_join(ref_jerseys, by = c("respRefId" = "officialId")) %>%
    rename(playerId = jerseyNum) %>%    
    group_by(gameId, playerId) %>%
    summarise(avg_dist = mean(distance, na.rm = TRUE), n_fouls = n()) %>%
    select(gameId, playerId, avg_dist, n_fouls) %>%
    collect()

season_stats <-
    shooting_fouls %>%
    mutate(t_d = avg_dist * n_fouls) %>%
    group_by(playerId) %>%
    summarise(t_dist = sum(t_d), t_fouls = sum(n_fouls)) %>%
    mutate(avg_dist = t_dist/t_fouls) %>%
    select(playerId, avg_dist)

shooting_fouls <- select(shooting_fouls, -n_fouls)

write_csv(shooting_fouls, "data/lead_halfcourt_games.csv")
write_csv(season_stats, "data/lead_halfcourt_season.csv")
