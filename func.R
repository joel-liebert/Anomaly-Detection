#TODO: Create own anomaly function
#TODO: Email results
#TODO: Automate script

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
  ticker_data  <- dbGetQuery(bqcon, paste0("WITH row_num_query AS (
                                              SELECT id, data_timestamp, data_value, granularity_item_id, index_id, createdate, ticker_index,
                                              ROW_NUMBER() OVER (PARTITION BY ticker_index ORDER BY data_timestamp DESC) AS ticker_row_num
                                              FROM (
                                                SELECT id, data_timestamp, data_value, granularity_item_id, index_id, createdate,
                                                CONCAT(index_id, '_', granularity_item_id) AS ticker_index
                                                FROM `freightwaves-data-factory.index_time_series.indx_index_data`
                                                WHERE data_timestamp <= TIMESTAMP('", target_date, "')
                                                )
                                              )
                                            SELECT rnq1.id, rnq1.data_timestamp, rnq1.data_value, rnq1.granularity_item_id,
                                              rnq1.index_id, rnq1.createdate, rnq1.ticker_index, rnq1.ticker_row_num, rnq2.max_row_num
                                            FROM row_num_query AS rnq1
                                            LEFT JOIN (
                                              SELECT ticker_index,
                                              MAX(ticker_row_num) AS max_row_num
                                              FROM row_num_query
                                              GROUP BY ticker_index
                                              ) AS rnq2
                                            ON rnq1.ticker_index = rnq2.ticker_index
                                            WHERE ticker_row_num <= ", days_of_data, "
                                              AND max_row_num    >= ", days_of_data, "
                                              AND ", where_clause))
  
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
detect_anomaly <- function(input_df, ticker_index) {
  
  # Load proper packages
  library(tibbletime)
  library(anomalize)
  library(timetk)
  
  # Preprocess dataframe and then perform anomaly calculations
  df <- input_df[input_df$ticker_index == ticker_index, ] %>% 
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
  frequency     <- as.numeric(ticker_data[df_len, 'data_timestamp'] - ticker_data[df_len-1, 'data_timestamp'])
  
  result <- data.frame(ticker_index=ticker_index, date=date, value=value, seven_day_avg=seven_day_avg,
                       score=score, anomaly=anomaly, repetitions=repetitions, frequency=frequency)
  
  return(result)
  
}


#---- Run Anomaly Detector on All Tickers ----
master_anomaly_detector <- function(ticker_data, ticker_gran, ticker_info) {
  
  start_time <- proc.time()
  
  # Create empty dataframe and then run the anomaly detector on every ticker
  anomaly_df <- data.frame(ticker_index=NULL, date=NULL, value=NULL, seven_day_avg=NULL,
                           score=NULL, anomaly=NULL, repetitions=NULL, frequency=NULL)
  
  # Get a dataframe of every ticker
  ticker_IDs <- ticker_data %>% distinct(ticker_index)
  
  result     <- apply(ticker_IDs, 1, function(x) detect_anomaly(ticker_data, x))
  
  anomaly_df <- lapply(result, function(x) rbind(anomaly_df, as.data.frame(x))) %>%
    bind_rows() %>%
    `rownames<-` (NULL)
  
  anomaly_df <- anomaly_df %>%
    separate(ticker_index, c('index', 'region'), sep = '_', remove = FALSE) %>%
    merge(ticker_gran, by.x = 'region', by.y = 'id', all.x = TRUE) %>%
    merge(ticker_info, by.x = 'index',  by.y = 'id', all.x = TRUE) %>%
    select(c(data_timestamp, index, region, ticker_index, value, seven_day_avg,
             score, anomaly, repetitions, frequency.x, frequency.y, granularity1, Description,
             index_name, ticker, description, display_unit_type, documentation_url)) %>% 
    rename(frequency        = frequency.x,
           alleged_freq     = frequency.y,
           granularity      = granularity1,
           granularity_desc = Description,
           ticker_desc      = description)

  time <- proc.time() - start_time
  print(paste0("Total elapsed time: ", round(time[[3]], 2), " seconds."))
  
  return(anomaly_df)
  
}
