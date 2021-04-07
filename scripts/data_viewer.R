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
scripts <- list.files(
    "./scripts/metrics", full.names = TRUE, include.dirs = FALSE
)
# do.call(file.remove, list(list.files("./data/", full.names = TRUE)))

for (metric in scripts) {
    print(metric)
    source(metric)
}

ref_name_map <-
    ref_jerseys %>%
    select(Name, playerId = jerseyNum, officialId) %>%
    distinct() %>%
    collect()

files <- list.files("./data", full.names = TRUE, include.dirs = FALSE)

season_data <- NULL
poss_data <- NULL
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
    } else if (str_detect(file, "_poss")) {
        if (is.null(poss_data)) {
            poss_data <- tracking_metric
        } else {
            poss_data <- full_join(poss_data, tracking_metric)
        }
    }
}

poss_data <-
    inner_join(ref_name_map, poss_data) %>%
    select(-playerId)

game_data <-
    inner_join(ref_name_map, game_data) %>%
    select(-playerId) 

season_data <-
    inner_join(ref_name_map, season_data) %>%
    select(-playerId) 

write_csv(poss_data, "data/poss_aggregate.csv")
write_csv(game_data, "data/game_aggregate.csv")
write_csv(season_data, "data/season_aggregate.csv")

dbWriteTable(
    sql_server,
    "referee_tracking_metrics_possession",
    poss_data,
    overwrite=TRUE
)

dbWriteTable(
    sql_server,
    "referee_tracking_metrics_game",
    game_data,
    overwrite=TRUE
)

dbWriteTable(
    sql_server,
    "referee_tracking_metrics_season",
    season_data,
    overwrite=TRUE
)

print(paste("Process run successfully on", Sys.time()))
