source("func.R")

data <- load_data(days_of_data = 16,
                  where_clause = "TRUE")
ticker_data = data[[1]]
ticker_gran = data[[2]]
ticker_info = data[[3]]

#---- FOR TESTING PURPOSES ONLY: Impute Random Fakes ---
ticker_count   <- n_distinct(ticker_data$granularity_item_id)
random_indices <- 16 * sort(sample.int(ticker_count, sample.int(ticker_count, 1)))
ticker_data    <- impute_fakes(ticker_data, random_indices, 0)
#----------------------------------------------------- -

# anomaly_df <- data.frame(ticker = NULL, value = NULL, seven_day_avg = NULL, score = NULL, anomaly = NULL)
anomaly_df <- master_anomaly_detector(ticker_data = ticker_data,
                                      ticker_gran = ticker_gran,
                                      ticker_info = ticker_info)
