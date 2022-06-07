#---- Load Packages ----
load_packages <- function() {
  
  library(tidyverse)
  library(bigrquery)
  library(googleCloudStorageR)
  library(DBI)
  
}


#---- Load Data ----
load_data <- function(days_of_data, target_date = Sys.Date(), where_clause = "TRUE") {
  
  start_time <- proc.time()
  
  load_packages()
  
  # Authenticate GCP / BQ
  bq_auth(Sys.getenv('gcp_credentials'))
  gcs_auth(Sys.getenv('gcp_credentials'))
  
  # Connect to project
  bqcon <- dbConnect(
    bigrquery::bigquery(),
    project = "freightwaves-data-factory"
  )
  
  # Read in ticker data
  ticker_data  <- dbGetQuery(bqcon, paste0("SELECT *
                                          FROM `freightwaves-data-factory.index_time_series.indx_index_data`
                                          WHERE data_timestamp BETWEEN TIMESTAMP(DATE_SUB(TIMESTAMP('", target_date, "'), INTERVAL ", days_of_data, " DAY))
                                                                   AND TIMESTAMP('", target_date, "')
                                           AND ",  where_clause))
  # Get rid of useless columns and turn appropriate values into factors
  ticker_data <- ticker_data %>% 
    mutate(ticker_index        = factor(paste(index_id, granularity_item_id, sep = "_")),
           id                  = factor(id),
           index_id            = factor(index_id)) %>%
    arrange(granularity_item_id, data_timestamp)
  
  ticker_gran <- dbGetQuery(bqcon, "SELECT *
                                  FROM `freightwaves-data-factory.index_time_series.indx_granularity_item`")
  
  ticker_info <- dbGetQuery(bqcon, "SELECT *
                                  FROM `freightwaves-data-factory.index_time_series.indx_index_definition`")
  
  time <- proc.time() - start_time
  print(paste0("Total elapsed time: ", round(time[[3]], 2), " seconds."))
  
  return(list(ticker_data, ticker_gran, ticker_info))
  
}


#---- Impute Fake Values ----
impute_fakes <- function(df, index, multiplication_factor) {
  
  # Multiply the value at the given index by a given factor
  df[index, "data_value"] <- df[index, "data_value"] * multiplication_factor
  
  return(df)
}


#---- Detect Anomalies ----
detect_anomaly <- function(input_df, ticker) {
  
  # Load proper packages
  library(tibbletime)
  library(anomalize)
  library(timetk)
  
  # Preprocess dataframe and then perform anomaly calculations
  df <- input_df[input_df$ticker_index == ticker, ] %>% 
    select(data_timestamp, data_value) %>% 
    arrange(data_timestamp) %>% 
    as_tibble() %>%
    time_decompose(data_value, merge = TRUE, message = FALSE, frequency = "1 week") %>%
    anomalize(remainder) %>%
    time_recompose() %>%
    rowwise() %>% 
    mutate(buffer_zero   = mean(remainder_l1, remainder_l2),
           buffer_radius = abs(remainder_l2 - buffer_zero),
           score = (abs(remainder) - buffer_zero) / buffer_radius)
  
  df_len        <- nrow(df)
  date          <- df[df_len, 'data_timestamp']
  value         <- as.numeric(df[df_len, 'data_value'])
  seven_day_avg <- as.numeric(mean(df$data_value[(df_len-7) : df_len]))
  score         <- as.numeric(df[df_len, 'score'])
  anomaly       <- as.character(df[df_len, 'anomaly'])
  repetitions   <- as.numeric(sum(df$data_value[(df_len-8) : df_len-1] == value))
  
  result <- data.frame(ticker = ticker, date = date, value = value, seven_day_avg = seven_day_avg, score = score, anomaly = anomaly, repetitions = repetitions)
  return(result)
  
}


#---- Run Anomaly Detector on All Tickers ----
master_anomaly_detector <- function(ticker_data, ticker_gran, ticker_info) {
  
  start_time <- proc.time()
  
  # Create empty dataframe and then run the anomaly detector on every ticker
  anomaly_df <- data.frame(ticker = NULL, date = NULL, value = NULL, seven_day_avg = NULL, score = NULL, anomaly = NULL, repetitions = NULL)
  
  # Get a dataframe of every ticker
  ticker_IDs <- ticker_data %>% distinct(ticker_index)
  
  result <- apply(ticker_IDs, 1, function(x) detect_anomaly(ticker_data, x))
  
  anomaly_df <- lapply(result, function(x) rbind(anomaly_df, as.data.frame(x))) %>%
    bind_rows() %>%
    `rownames<-` (NULL)
  
  anomaly_df <- anomaly_df %>%
    separate(ticker, c('index', 'region'), sep = '_', remove = FALSE) %>%
    merge(ticker_gran, by.x = 'region', by.y = 'id', all.x = TRUE) %>%
    merge(ticker_info, by.x = 'index',  by.y = 'id', all.x = TRUE) %>%
    select(c(data_timestamp, index, region, ticker.x, value, seven_day_avg, score, anomaly, repetitions, granularity1, Description, index_name, ticker.y, description, display_unit_type, documentation_url)) %>% 
    rename(ticker           = ticker.x,
           ticker_index     = ticker.y,
           granularity      = granularity1,
           granularity_desc = Description,
           ticker_desc      = description)

  
  time <- proc.time() - start_time
  print(paste0("Total elapsed time: ", round(time[[3]], 2), " seconds."))
  
  return(anomaly_df)
  
}