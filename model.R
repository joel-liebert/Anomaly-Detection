source("C:/Users/jliebert/Documents/projects/Anomaly-Detection/func.R")

days_of_data <- 28

data <- load_data(days_of_data = days_of_data,
                  target_date  = Sys.Date(),
                  where_clause = "index_id = 9 AND granularity_item_id < 200")
ticker_data = data[[1]]
ticker_gran = data[[2]]
ticker_info = data[[3]]

#---- FOR TESTING PURPOSES ONLY: Impute Random Fakes ---
ticker_count   <- n_distinct(ticker_data$granularity_item_id)
random_indices <- days_of_data * sort(sample.int(ticker_count, sample.int(ticker_count, 1)))
ticker_data    <- impute_fakes(ticker_data, random_indices, 0)
#----------------------------------------------------- -

# How many records of data are there for each ticker?
# ticker_occurrence <- ticker_data %>%
#   count(ticker_index, name = 'count')
# 
# # Find the top tickers by merging the ticker occurrences and the ticker dataframe 
# top_tickers <- ticker_data %>%
#   merge(ticker_occurrence, by = "ticker_index", all.x = TRUE) %>%
#   filter(count > days_of_data-1)

# anomaly_df <- data.frame(ticker = NULL, value = NULL, seven_day_avg = NULL, score = NULL, anomaly = NULL)
anomaly_df <- master_anomaly_detector(ticker_data = ticker_data,
                                      ticker_gran = ticker_gran,
                                      ticker_info = ticker_info)

