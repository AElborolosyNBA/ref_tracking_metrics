# For every review, flag if the ref committed a mechanical error in that moment.
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
library(stringr)

source("scripts/helper.R")

flag_errors <- function(
    gbq_con, sql_con, call_type, is_update, included_dates, included_gid
) {
    #' @description 
    #' Acquire reviews and organize by halfcourt vs transition & role (LST)
    #' Error List:
    #' Halfcourt Lead: Is Lead Wide?
    #' Halfcourt Slot: Is Slot Above/Below/At FT Line
    #' Halfcourt Trail: Is Trail by 28 foot mark/behind ball?
    #' Transition Lead: Is lead at the basket yet?
    #' Transition Slot: Is the slot by the middle of the pack?
    #' Transition Trail: Is the trail behind the ball?
    #' 
    #' @param gbq_con (DBI Connection Obj): Connection to GBQ
    #' 
    #' @param sql_con (DBI Connection Obj): Connection to SQL Server
    #' 
    #' @param is_update (Bool): Get entire dataset or just new games.
    #' 
    #' @param included_dates (Char Vector): dates to use for is_update mode
    #' 
    #' @param included_gid (Char Vector): games to use for is_update mode
    #' 
    #' @return flags (Tibble) Flagged reviews
    season_regex <- filter_games("_")
    current_season <- identify_season()
    
    middle_of_pack <-
        gbq_reviews %>%
        filter(respRefLoc == 'Slot') %>%
        select(gameDate, gameId, eventId, ratingSeqNo, wcTime) %>%
        inner_join(., track) %>%
        filter(!(teamId %in% c(-1, 0))) %>%
        group_by(gameDate, gameId, eventId, ratingSeqNo) %>%
        summarise(middle_of_pack = sql("approx_quantiles(x, 2)[offset(1)]")) %>%
        ungroup()

    flags <-
        track %>%
        filter(teamId == 0) %>%
        mutate(season = as.integer(paste0("20", substr(gameId, 4, 5)))) %>%
        inner_join(ref_jerseys) %>%
        select(
            gameDate, gameId, wcTime, respRefId = officialId, x, y, ballX
        ) %>%
        inner_join(gbq_reviews) %>%
        inner_join(
            poss, by = c("gameDate", "gameId", "possNum", "period")
        ) %>%
        left_join(middle_of_pack) %>%
        mutate(
            callType = case_when(
                (playerAction %in% c("INF", "WPA") & called == "C") ~ "CC",
                (playerAction %in% c("INF") & called == "NC") ~ "INC",
                (
                    playerAction %in% c("NI", "SFA", "PFA", "BCA") &
                    called == "C"
                ) ~ "IC",
                (
                    playerAction %in% c("NI", "SFA", "PFA", "BCA") &
                    called == "NC"
                ) ~ "CNC",
                TRUE ~ "None"
            ),
            loc = if_else(
                sign(ballX) == sign(basketX), "Halfcourt", "Transition"
            ),
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
                    x < middle_of_pack - (3*sign(basketX)), 0, 1
                ),
                loc == "Transition" & respRefLoc == "Trail" ~ ifelse(
                    x < ballX - (3*sign(basketX)), 0, 1
                ),
                TRUE ~ 3 # Number code for no flag covers this.
            ),
            mechanic = case_when(
                loc == "Halfcourt" & respRefLoc == "Lead" ~
                    "Wide Lead",
                loc == "Halfcourt" & respRefLoc == "Slot" ~
                    "By FT Line Ext",
                loc == "Halfcourt" & respRefLoc == "Trail" ~
                    "Trailing Ball - Halfcourt",
                loc == "Transition" & respRefLoc == "Lead" ~
                    "Lead at Basket",
                loc == "Transition" & respRefLoc == "Slot" ~ "By the Pack",
                loc == "Transition" & respRefLoc == "Trail" ~
                    "Trailing Ball - Transition"
            )
        ) %>%
        filter(
            callType %in% c("CC", "IC", "INC"),
            respRefLoc %in% c("Lead", "Slot", "Trail")
        ) %>%
        select(
            game_date = gameDate,
            game_id = gameId,
            event_id = eventId,
            rating_seq_no = ratingSeqNo,
            role = respRefLoc,
            call_type = callType,
            loc, flagged, mechanic
        )

    if (is_update) {
        flags <-
            flags %>%
            filter(
                game_id %LIKE% season_regex,
                # (game_date %in% included_dates),
                game_id %in% included_gid
            )
    }
    
    if (!is.null(call_type)) {
        flags <-
            flags %>%
            filter(call_type %in% call_type)
    }
    
    flags <- collect(flags)
    
    # Join reviews back to their video clips----
    video_clips <-
        dbGetQuery(
            sql_server,
            "
            SELECT
	            game_id,
	            eventId AS event_id,
	            ratingSeqNo AS rating_seq_no,
	            CONCAT(
		            'clips.nba.com/',
                    SUBSTRING(
            			VideoURL_UTC,
			            CHARINDEX('?gameNo=', VideoURL_UTC),
			            LEN(VideoURL_UTC)
		            )
	            ) AS video_url
            FROM
	            db_NBA_RefOps.dbo.GRS3_ALLOGRColumns_UTC
            "
        )
        
    results <-
        inner_join(flags, video_clips) %>%
        select(
            game_id, event_id, rating_seq_no, call_type,
            role, loc, flagged, mechanic, video_url
        ) %>%
        filter(between(flagged, 0, 1))
}

update_upload_flags <- function(
    gbq_con, sql_server, is_update, call_type
) {
    if (is_update) {
        
        missing_data <-
            dbGetQuery(
                sql_server,
                paste0(
                "
                SELECT
                	DISTINCT CAST(g.Date_EST AS DATE) AS date_est,
                    g.game_id
                FROM
                	referee_tracking_metrics_flags rtmf
                RIGHT JOIN
                	db_NBA_mCoreStats.dbo.Game g
                	ON g.Game_id = rtmf.game_id
                WHERE
                	rtmf.Game_id IS NULL
                	AND g.League_id = '00'
                ",
                "AND g.game_id LIKE '", filter_games("_"), "'"
                )
            )
        
        missing_dates <- missing_data %>% pull(date_est)
        missing_games <- missing_data %>% pull(game_id)

        flags <- flag_errors(
            gbq, sql_server, call_type, TRUE, missing_dates, missing_games
        )
        
        dbWriteTable(
            conn=sql_server,
            name="referee_tracking_metrics_flags",
            value=flags,
            overwrite=FALSE,
            append=TRUE
        )
        
    } else {
        flags <- flag_errors(gbq, sql_server, call_type, FALSE, NULL)
        
        dbWriteTable(
            conn=sql_server,
            name="referee_tracking_metrics_flags",
            value=flags,
            overwrite=TRUE,
            field.types=c(
                game_id="CHAR(10)",
                event_id="INT",
                rating_seq_no="TINYINT",
                call_type="VARCHAR(3)",
                role="VARCHAR(5)",
                loc="VARCHAR(10)",
                flagged="TINYINT",
                mechanic="VARCHAR(26)",
                video_url="VARCHAR(100)"
            )
        )
    }
}


# update_upload_flags(gbq, sql_server, is_update = FALSE, call_type = NULL)
update_upload_flags(gbq, sql_server, is_update = TRUE, call_type = NULL)
print(paste("Process run successfully on", Sys.time()))
