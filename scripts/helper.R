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

current_year <- format(Sys.Date(), "%Y")
current_month <- format(Sys.Date(), "%m")
current_season <- NULL

if (current_month >= 9) {
    current_season <- as.integer(current_year)
} else {
    current_season <- as.integer(current_year) - 1
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