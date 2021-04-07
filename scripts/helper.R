# Helper module that contains shared functions across metrics. ----

# Common Libraries and DB Connections
library(checkpoint)
checkpoint("2019-12-30", verbose = FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(ggplot2)
library(tidyr)

options(scipen = 20)

identify_season <- function() {
    #' @description Identify which NBA Season we're in,
    #' assuming preseason starts in September and the season ends the following
    #' calendar year.
    #' 
    #' @return season (Int): The first year of the season -- 2020-2021
    #' season returns 2020.

    current_year <- format(Sys.Date(), "%Y")
    current_month <- as.integer(format(Sys.Date(), "%m"))
    current_season <- NULL
    
    if (current_month >= 9) {
        current_season <- as.integer(current_year)
    } else {
        current_season <- as.integer(current_year) - 1
    }
    
    return(current_season)
}

sql_server <-  dbConnect(
    odbc::odbc(),
    .connection_string = paste0(
        "driver=ODBC Driver 17 for SQL Server;", 
        "server=", Sys.getenv("SERVER"), ";UID=", Sys.getenv("USER"), 
        ";Pwd=", Sys.getenv("PASS"), ";Database=db_NBA_BSA")
)

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

# Function to list all the referees
list_referees <- function(sql_server) {
    dbGetQuery(sql_server, "EXEC [sp_grs_referee_leaderboard]")
}

# Identify L/S/T Ref for each possession of a game.
identify_ref_position <- function(gbq) {
    DBI::dbGetQuery(
        gbq,
        "
        SELECT
            gameDate,
        	gameId,
            possNum,
	        playerId,
	        officialId,
            season,
	        CASE WHEN dist_rank = 1 THEN 'Lead'
	             WHEN dist_rank = 2 THEN 'Slot'
	             WHEN dist_rank = 3 THEN 'Trail' END AS playerType
        FROM
	        (
	        SELECT
		        track.gameDate,
		        track.gameId,
                poss.possNum,
		        track.playerId,
		        jersey.officialId,
                jersey.season,
		        row_number() OVER(
		            PARTITION BY track.gameId, track.wcTime, track.frameId
	                ORDER BY
		                SQRT(POW(x - basketX, 2) + POW(y, 2))) AS dist_rank
	        FROM
		        `nba-tracking-data.NbaPlayerTracking.Tracking` track
	        INNER JOIN `bball-strategy-analytics.GrsReviews.JerseyLookUp` jersey ON
		        track.playerId = jersey.jerseyNum
	        INNER JOIN `nba-tracking-data.NbaPlayerTracking.Possessions` poss ON
		        poss.gameDate = track.gameDate
		        AND poss.gameId = track.gameId
		        AND track.wcTime BETWEEN poss.wcStart AND poss.wcEnd
	        WHERE
                SUBSTR(track.gameId, 4, 2) = SUBSTR(CAST(jersey.season AS STRING), 3, 2)
                AND track.teamId = 0
                -- Remove possessions shorter than 3 seconds
                AND track.wcTime = poss.wcStart + 3000) poss_info
        WHERE
            dist_rank <= 3 -- 3 Referees
        ORDER BY
            gameDate, gameId, possNum
        "
    ) %>%
        select(-season)
}

filter_games <- function(season_filt) {
    #' Helper function that constructs the regex for games in the current season.
    #'
    #' @param season_filt (numeric) Number for season type
    #'     1: Preseason
    #'     2: Regular Season
    #'     3: Allstar
    #'     4: Playoffs
    #'     _: All of the above
    #'     Multiple selection permitted ex: c(1, 2)
    #'
    #' @return (char) regex to match against gbq GRS Reviews table.
    curr_year <- as.numeric(format(Sys.Date(), "%Y"))
    curr_month <- as.numeric(format(Sys.Date(), "%m"))
    if (curr_month >= 9) {
        season <- substr(curr_year, nchar(curr_year)-1, nchar(curr_year))
    } else {
        season <- substr(curr_year-1, nchar(curr_year)-1, nchar(curr_year))
    }
    
    if (length(season_filt) > 1) {
        season_filt <- paste0("[", paste0(season_filt, collapse = ""), "]")
    }
    
    paste0("00", season_filt, season, "%")
}

create_flags_dataset <- function(
    flags, reviews, jerseys,
    is_update, ignored_gid, gid_regex, current_season
)
{
    #' Holds the query for the grs reviews w/ columns renamed. 
    #' 
    #' @param flags (DBI Table Object) Connection to MS SQL flag Table
    #' 
    #' @param reviews (DBI Table Object) Connection to MS SQL GRS Table
    #' 
    #' @param jersey (DBI Conn Object) Connection to the GBQ Ref Jersey Table
    #' 
    #' @param track (DBI Conn Object) Connection to the GBQ Tracking Data Table
    #' 
    #' @param poss (DBI Conn Object) Connection to the GBQ Possession Data Table
    #' 
    #' @param is_update (BOOL) If the dataset is an update or not.
    #' 
    #' @param ignored_gid (char vector) OPTIONAL. GIDs to skip. Mandatory if
    #' is_update is true.
    #' @param gid_regex (char) OPTIONAL, regex for games this year. Mandatory if
    #' is_update is true.
    #' 
    #' @return reviews (Tibble) Reviews Data ready to upload.
    if (is_update) {
        assert_that(
            !(missing(ignored_gid) || missing(gid_regex)),
            msg = "is_update=TRUE, provide ignored_gid & gid_regex arguments."
        )
    }
    
    reviews <-
        reviews %>%
        select(
            game_id = gameId,
            event_id = eventId,
            rating_seq_no = RatingSeqNo,
            video_url = VideoURL_UTC
        )
    
    if (is_update) {
        reviews <-
            reviews %>%
            filter(!(gameId %in% ignored_gid), gameId %like% gid_regex)
    }
    
    reviews <- reviews %>% collect()

    flags <-
        track %>%
        filter(teamId == 0) %>%
        mutate(season = paste0("20", substr(gameId, 4, 5))) %>%
        inner_join(jerseys) %>%
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
                TRUE ~ 3 # Number code for non-existent flag
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
    
    return(reviews)
}
