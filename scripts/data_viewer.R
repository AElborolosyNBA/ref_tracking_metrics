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

# Update all the metric files.
scripts <- paste0(
    "./scripts/metrics/",
    list.files("./scripts/metrics/")
)

for (metric in scripts) {
    print(metric)
    source(metric)
}

files <- paste0(
    "./data/",
    list.files("./data/")
)

ref_name_map <-
    ref_jerseys %>%
    select(Name, playerId = jerseyNum, officialId) %>%
    distinct() %>%
    collect()

season_data <- NULL
game_data <- NULL

for (file in files) {
    print(file)
    tracking_metric <- read_csv(file)
    if (str_detect(file, "_games")) {
        if (is.null(game_data)) {
            game_data <- tracking_metric
        } else {
            game_data <- full_join(game_data, tracking_metric)
        }
    } else if (str_detect(file, "_season")) {
        if (is.null(season_data)) {
            season_data <- tracking_metric
        } else {
            season_data <- full_join(season_data, tracking_metric)
        }
    }
}

game_data <-
    inner_join(ref_name_map, game_data) %>%
    select(-playerId) 

season_data <-
    inner_join(ref_name_map, season_data) %>%
    select(-playerId) 

write_csv(season_data, "data/season_aggregate.csv")
write_csv(game_data, "data/game_aggregate.csv")

dbWriteTable(
    sql_server,
    "referee_tracking_metrics_season_dev",
    season_data,
    overwrite=TRUE
)

dbWriteTable(
    sql_server,
    "referee_tracking_metrics_game_dev",
    game_data,
    overwrite=TRUE
)

print(paste("Process run successfully on", Sys.time()))
