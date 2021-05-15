# View the game level and season level performance stats and upload to db
library(checkpoint)
checkpoint("2019-12-30", verbose=FALSE)

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(stringr)
library(tidyr)

options(gargle_oauth_email = TRUE)

#' New Process would look like
#' 1. Identify games to exclude
#' 2. For every metric:
#' 2a. Call poss level function.
#' 2b. Append to master poss level data
#' 2c. Spread data into wide format.
#' 3. Append new poss data to poss table
#' 4. Game/Season level data are views of poss table
#' 4a. Game could stay as a table since data is being added at game level.

update_metric_data <- function() {
    #' Updates all the metrics files by running their respective scripts.
    #'
    #' @return NA
    
    scripts <- list.files(
        "./scripts/metrics", full.names = TRUE, include.dirs = FALSE
    )
    scripts <- scripts[grep("\\.R$", scripts)]
    
    for (metric in scripts) {
        print(metric)
        source(metric)
    }
    
    return(NA)
}

join_metrics <- function(file_regex) {
    #' Join metric datas into a single tibble.
    #' 
    #' @param file_regex (char vector) Regex to use to identify files.
    #' 
    #' @return metrics (tibble) Tibble of metrics data joined together.
    #' 
    files <- list.files(
        "./data/", full.names = TRUE, include.dirs = FALSE
    )
    files <- files[grep(file_regex, files)]
    
    metrics <- NULL
    for (file in files) {
        print(file)
        tracking_metric <- read_csv(file)
        if (is.null(metrics)) {
            metrics <- tracking_metric
        } else {
            metrics <- full_join(metrics, tracking_metric)
        }
    }
    
    return(metrics)
}

update_table <- function(
    table_name, data_regex, ref_name_map, agg_file, sql_con
) {
    #' Update DB Table w/ refreshed metric data
    #' 
    #' @param table_name (char vector) Name of DB Table
    #' @param data_regex (char vector) Regex used to find mechanic files
    #' @param ref_name_map (Tibble) Mapping of ref IDs to their names
    #' @param agg_file (char vector) Name of file to save joined data to.
    #' @param sql_con (DBI Con) Connection to db_NBA_BSA database.
    #' 
    #' @return NA
    
    metrics <-
        join_metrics(data_regex) %>%
        inner_join(ref_name_map) %>%
        select(-playerId)
    
    write_csv(metrics, agg_file)
    dbWriteTable(sql_con, table_name, metrics, overwrite=TRUE)
    
    return(metrics)
}

source("scripts/helper.R")

ref_name_map <-
    ref_jerseys %>%
    select(Name, playerId = jerseyNum, officialId) %>%
    distinct() %>%
    collect()

update_metric_data()

poss <-
    join_metrics("_poss\\.csv$") %>%
    inner_join(ref_name_map) %>%
    select(-playerId)
print(poss)
bq_perform_upload(
    x = "bball-strategy-analytics.GrsReviews.tracking_metrics_possession",
    fields = as_bq_fields(poss),
    values = poss,
    create_disposition = "CREATE_IF_NEEDED",
    write_disposition = "WRITE_TRUNCATE"
)

update_table(
    "referee_tracking_metrics_possession", "_poss\\.csv$",
    ref_name_map, "data/poss_aggregate.csv", sql_server
)

update_table(
    "referee_tracking_metrics_game", "_games\\.csv$",
    ref_name_map, "data/game_aggregate.csv", sql_server
)

update_table(
    "referee_tracking_metrics_season", "_season\\.csv$",
    ref_name_map, "data/season_aggregate.csv", sql_server
)

print(paste("Process run successfully on", Sys.time()))
