# View the game level and season level performance stats
library(checkpoint)
checkpoint("2019-12-30")

library(bigrquery)
library(dbplyr)
library(DBI)
library(dplyr)
library(readr)
library(stringr)
library(tidyr)

files <- paste0(
    "./data/",
    list.files("./data/")
)

ref_name_map <- ref_jerseys %>% select(Name, playerId = jerseyNum) %>% collect()

season_data <- NULL
game_data <- NULL

for (file in files) {
    tracking_metric <- read_csv(file)
    if (str_detect(file, "games")) {
        if (is.null(game_data)) {
            game_data <- tracking_metric
        } else {
            game_data <- full_join(game_data, tracking_metric)
        }
    } else if (str_detect(file, "season")) {
        if (is.null(season_data)) {
            season_data <- tracking_metric
        } else {
            season_data <- full_join(season_data, tracking_metric)
        }
    }
}

season_data <- inner_join(ref_name_map, season_data)
game_data <- inner_join(ref_name_map, game_data)

View(season_data)
View(game_data)

write_csv(season_data, "data/season_aggregate.csv")
write_csv(game_data, "data/game_aggregate.csv")