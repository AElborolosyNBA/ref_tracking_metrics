# For every review, flag if the ref committed a mechanical error in that moment.
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

# Acquire reviews and organize by event type (halfcourt vs transition, LST) ----
# Error List:
# Halfcourt Lead: Is Lead Wide?
# Halfcourt Slot: Is Slot Above/Below/At FT Line
# Halfcourt Trail: Is Trail by 28 foot mark/behind ball?
# Transition Lead: Is lead at the basket yet?
# Transition Slot: Is the slot by the middle of the pack?
# Transition Trail: Is the trail behind the ball?

season_regex <- paste0("00_", substr(as.character(current_season), 3, 4), "%")

middle_of_pack <-
    dbGetQuery(
        gbq,
        paste0(
        "
SELECT
    DISTINCT track.gameId,
    grs.eventId,
    grs.ratingSeqNo,
    PERCENTILE_CONT(x, 0.5) OVER(
        PARTITION BY track.gameDate, track.gameId, track.wcTime) AS middle_of_pack
FROM
    `nba-tracking-data.NbaPlayerTracking.Tracking` track
INNER JOIN
    `bball-strategy-analytics.GrsReviews.Reviews` grs
    ON grs.gameDate = track.gameDate
    AND grs.gameId = track.gameId
    AND grs.wcTime = track.wcTime
WHERE
    track.gameId LIKE '", season_regex, "'",
        "
    AND teamId NOT IN (-1, 0)
    AND grs.respRefLoc = 'Slot'
        "
        )
    )

reviews <-
    track %>%
    filter(gameId %LIKE% season_regex, teamId == 0) %>%
    inner_join(ref_jerseys) %>%
    filter(season == current_season) %>%
    select(gameDate, gameId, wcTime, respRefId = officialId, x, y, ballX) %>%
    inner_join(gbq_reviews) %>%
    inner_join(poss, by = c("gameDate", "gameId", "possNum", "period")) %>%
    mutate(
        callType = case_when(
            (playerAction %in% c("INF", "WPA") & called == "C") ~ "CC",
            (playerAction %in% c("INF") & called == "NC") ~ "INC",
            (playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "C") ~ "IC",
            (playerAction %in% c("NI", "SFA", "PFA", "BCA") & called == "NC") ~ "CNC",
            TRUE ~ "None"
        ),
        loc = if_else(sign(ballX) == sign(basketX), "Halfcourt", "Transition"),
        flagged = case_when(
            loc == "Halfcourt" & respRefLoc == "Lead" ~ ifelse(
                abs(y) > 12, 0, 1
            ),
            loc == "Halfcourt" & respRefLoc == "Slot" ~ ifelse(
                between(abs(x), 25, 31), 0, 1
            ),
            loc == "Halfcourt" & respRefLoc == "Trail" ~ ifelse(
                x < ballX - (3*sign(basketX)), 0, 1
            ),
            loc == "Transition" & respRefLoc == "Lead" ~ ifelse(
                abs(x) > 44, 0, 1
            ),
            loc == "Transition" & respRefLoc == "Slot" ~ ifelse(
                TRUE, 2, 0 # Mark as 2 for easy fixing later
            ),
            loc == "Transition" & respRefLoc == "Trail" ~ ifelse(
                x < ballX - (3*sign(basketX)), 0, 1
            ),
            TRUE ~ 3 # Number code for 
        ),
        mechanic = case_when(
            loc == "Halfcourt" & respRefLoc == "Lead" ~ "Wide Lead",
            loc == "Halfcourt" & respRefLoc == "Slot" ~ "By FT Line Ext",
            loc == "Halfcourt" & respRefLoc == "Trail" ~ "Trailing Ball - Halfcourt",
            loc == "Transition" & respRefLoc == "Lead" ~ "Lead at Basket",
            loc == "Transition" & respRefLoc == "Slot" ~ "By the Pack",
            loc == "Transition" & respRefLoc == "Trail" ~ "Trailing Ball - Transition"
        )
    ) %>%
    filter(
        callType %in% c("CC", "IC", "INC"),
        respRefLoc %in% c("Lead", "Slot", "Trail")
    ) %>%
    select(
        gameId, eventId, ratingSeqNo, infractionType, officialId = respRefId,
        role = respRefLoc, callType, loc, flagged, mechanic, x, basketX
    ) %>%
    collect() %>%
    left_join(middle_of_pack) %>%
    mutate(
        flagged = ifelse(
            flagged == 2,
            ifelse(x < middle_of_pack - (3*sign(basketX)), 0, 1),
            flagged
        )
    ) %>%
    select(
        gameId, eventId, ratingSeqNo, infractionType, officialId,
        role, callType, loc, flagged, mechanic
    )

# Join reviews back to their video clips----
video_clips <-
    dbGetQuery(
        sql_server,
        paste0(
            "
            SELECT
                gameId = game_id,
                eventId,
                ratingSeqNo,
                respRefLoc = ref_location,
                VideoURL_UTC
            FROM
	            db_NBA_RefOps.dbo.GRS3_ALLOGRColumns_UTC
            WHERE
                game_id LIKE '",
            season_regex,
            "'"
        )
    )

# Swap videoURL to clips.nba.com
intime <- str_extract(video_clips$VideoURL_UTC, "(?<=inTime=)[0-9]*(?=&)")
outtime <- str_extract(video_clips$VideoURL_UTC, "(?<=outTime=)[0-9]*(?=&)")
video_clips$VideoURL_UTC <- paste0(
    "https://clips.nba.com/?gameNo=",
    video_clips$game_id,
    "&inTime=",
    intime,
    "&outTime=",
    outtime,
    "&source=grs"
)
video_clips <- as_tibble(video_clips)

errors_per_game <-
    reviews %>%
    mutate(
        callError = ifelse(callType %in% c("IC", "INC"), 1, 0),
        mechanicError = ifelse(flagged == 1, 1, 0),
        doubleError = ifelse(callError & mechanicError, 1, 0)
    ) %>%
    group_by(gameId, officialId) %>%
    summarise_at(vars(contains("Error")), sum, na.rm = TRUE) %>%
    ungroup()
