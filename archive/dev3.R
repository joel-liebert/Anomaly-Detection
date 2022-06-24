test_data2 <- test2_1[, names(test2_1) %in% c("data_timestamp", "data_value")] %>% arrange(data_timestamp)
test_data3 <- test3_1[, names(test3_1) %in% c("data_timestamp", "data_value")] %>% arrange(data_timestamp)

test_data2 %>% time_decompose.tbl_time(target=data_value, frequency = "7 weeks")

data2 <- test_data2
data3 <- test_data3
# target_expr <- as.numeric(test_data$data_value)


time_decompose.tbl_time <- function(data, target, method = c("stl", "twitter"),
                                    frequency = "auto", trend = "auto", ..., merge = FALSE, message = TRUE) {
  
  # Checks
  if (missing(target)) stop('Error in time_decompose(): argument "target" is missing, with no default', call. = FALSE)
  
  # Setup
  target_expr <- dplyr::enquo(target)
  method      <- tolower(method[[1]])
  
  # Set method
  if (method == "twitter") {
    decomp_tbl <- data %>%
      decompose_twitter(!! target_expr, frequency = frequency, trend = trend, message = message, ...)
  } else if (method == "stl") {
    decomp_tbl <- data %>%
      decompose_stl(!! target_expr, frequency = frequency, trend = trend, message = message, ...)
    # } else if (method == "multiplicative") {
    #     decomp_tbl <- data %>%
    #         decompose_multiplicative(!! target_expr, frequency = frequency, message = message, ...)
  } else {
    stop(paste0("method = '", method[[1]], "' is not a valid option."))
  }
  
  # Merge if desired
  if (merge) {
    ret <- merge_two_tibbles(data, decomp_tbl, .f = time_decompose)
  } else {
    ret <- decomp_tbl
  }
  
  return(ret)
  
}

decompose_stl <- function(data, target, frequency = "auto", trend = "auto", message = TRUE) {
  
  # Checks
  if (missing(target)) stop('Error in decompose_stl(): argument "target" is missing, with no default', call. = FALSE)
  
  
  data <- prep_tbl_time(data)
  date_col_vals <- tibbletime::get_index_col(data)
  
  target_expr <- dplyr::enquo(target)
  
  date_col_name <- timetk::tk_get_timeseries_variables(data)[[1]]
  date_col_expr <- rlang::sym(date_col_name)
  
  freq <- time_frequency(data, period = frequency, message = message)
  trnd <- time_trend(data, period = trend, message = message)
  
  # Time Series Decomposition
  decomp_tbl <- data %>%
    dplyr::pull(!! target_expr) %>%
    stats::ts(frequency = freq) %>%
    stats::stl(s.window = "periodic", t.window = trnd, robust = TRUE) %>%
    sweep::sw_tidy_decomp() %>%
    # forecast::mstl() %>%
    # as.tibble() %>%
    tibble::add_column(!! date_col_name := date_col_vals, .after = 0) %>%
    dplyr::select(!! date_col_expr, observed, season, trend, remainder)
  
  decomp_tbl <- anomalize::prep_tbl_time(decomp_tbl)
  
  return(decomp_tbl)
  
}
