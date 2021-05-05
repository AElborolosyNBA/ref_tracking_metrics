# Generic process for creating a metric for uploading to the table.

format_metric <- function(metric, col) {
    #' Format metric to meet requirements for appending to master table.
    #' 
    #' @param metric (tibble): Metric data at possession level
    #' 
    #' @param col (string): Name of column w/ the metric
    #' 
    #' @return metric (tibble): metric data in long format
    long_metric <-
        metric %>%
        mutate(
            metric_name = !!col,
            metric_value = .[[col]]
        ) %>%
        select(gameId, possNum, officialId, metric_name, metric_value)
    
    return(long_metric)
}

create_metric <- function(query, col, is_update, excluded_gid) {
    #' Given a query, return the appropriate metric.
    #' 
    #' @param query (Modified DBI object) DBI table that results in the gameId,
    #' possNum, playerId (of ref), and the metric. 
    #' 
    #' @param col (String) Name of the metric column.
    #' 
    #' @param excluded_gid (Char Vector): Optional, game IDs to exclude
    #' 
    #' @param is_update (Bool)
    season_regex <- filter_games("_")
    current_season <- identify_season()
    
    if (is_update) {
        query <-
            query %>%
            filter(
                game_id %LIKE% season_regex,
                !(game_id %in% excluded_gid)
            )
    }
    
    query <-
        query %>%
        mutate(metric_name = !!col, metric_value = .[[col]]) %>%
        collect() %>%
        group_by(gameId, possNum, metric_name) %>%
        slice(which.max(metric_value))
        
    return(query)
}

#' End Goal:
#' row_bind(metrics...) %>%
#' spread(key = metric_name, value = metric_value) %>%
#' inner_join(ref_jersey_map)
season_regex <- filter_games("_")
current_season <- identify_season()

games_processed <-
    tbl(sql_server, "referee_tracking_metrics_game") %>%
    distinct(gameDate, gameId) %>%
    filter(gameId %LIKE% !!season_regex) %>%
    distinct(gameDate) %>%
    collect()

trail_transition_metric <-
    track %>%
    filter(teamId == 0) %>%
    select(gameDate, gameId, period, playerId, wcTime, x, ballX) %>%
    inner_join(., poss) %>%
    filter(between(wcTime, wcStart, wcEnd), sign(ballX) != sign(basketX)) %>%
    mutate(behind_ball = ifelse(x < ballX - (3*sign(basketX)), 1,0)) %>%
    group_by(gameId, possNum, playerId) %>%
    summarise(
        behind_ball = sum(behind_ball, na.rm = TRUE),
        n_frames = n(),
        perc_time_behind_ball_transition = behind_ball/n_frames
    ) %>%
    ungroup() %>%
    select(game_id, possNum, playerId, perc_time_behind_ball_transition)

trail_halfcourt_metric <-
    track %>%
    filter(teamId == 0) %>%
    select(gameDate, gameId, period, playerId, wcTime, x, ballX) %>%
    inner_join(., poss) %>%
    filter(between(wcTime, wcStart, wcEnd), sign(ballX) == sign(basketX)) %>%
    mutate(behind_ball = ifelse(x < ballX - (3*sign(basketX)), 1,0)) %>%
    group_by(gameId, possNum, playerId) %>%
    summarise(
        behind_ball = sum(behind_ball, na.rm = TRUE),
        n_frames = n(),
        perc_time_behind_ball_transition = behind_ball/n_frames
    ) %>%
    ungroup() %>%
    select(gameId, possNum, playerId, perc_time_behind_ball_halfcourt)

lead_halfcourt_metric <-
    track %>%
    filter(teamId == 0) %>%
    select(gameDate, gameId, period, playerId, wcTime, y, ballX) %>%
    inner_join(., poss) %>%
    filter(between(wcTime, wcStart, wcEnd), sign(ballX) == sign(basketX)) %>%
    mutate(is_wide = ifelse(abs(y) >= 12, 1, 0)) %>%
    group_by(gameId, possNum, playerId) %>%
    summarise(
        is_wide = sum(is_wide, na.rm = TRUE),
        n_frames = n(),
        perc_time_in_base_position_lead = is_wide/n_frames
    ) %>%
    ungroup() %>%
    select(gameId, possNum, playerId, perc_time_in_base_position_lead)


slot_halfcourt_metric <-
    track %>%
    filter(teamId == 0) %>%
    select(gameDate, gameId, period, playerId, wcTime, x, ballX) %>%
    inner_join(., poss) %>%
    filter(between(wcTime, wcStart, wcEnd), sign(ballX) == sign(basketX)) %>%
    mutate(by_ft_line = ifelse(between(abs(x), 25, 31), 1, 0)) %>%
    summarise(
        by_ft_line = sum(by_ft_line, na.rm = TRUE),
        n_frames = n(),
        perc_time_by_ft_line_ext = by_ft_line/n_frames
    ) %>%
    ungroup() %>%
    select(gameId, possNum, playerId, perc_time_by_ft_line_ext)

