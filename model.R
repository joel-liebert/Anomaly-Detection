# TODO: optimize time
# TODO: get rid of red warnings
# TODO: create function for testing
# TODO: ask Ray about what it means to put everything in a function

#---- Useful Code ----
# Time Code
# ptm <- proc.time()
# proc.time() - ptm

# Train/Test Split
# set.seed(123)
# smp_size  <- floor(0.75 * nrow(box_score))
# train_ind <- sample(seq_len(nrow(box_score)), size = smp_size)
# train <- box_score[train_ind,  -c(1, 2, ncol(box_score)-1)]
# test  <- box_score[-train_ind, -c(1, 2, ncol(box_score)-1)]

# Remove NAs
# index <- apply(df, 2, function(x) any(is.na(x)))
# colnames(df[index])
# df <- df[complete.cases(df$feature), ]
# df %>% drop_na()

# Select All Columns Except
# features <- as.data.frame(box_score[, !names(box_score) %in% c("game_id", "posteam", "winner_name", "winner")])
# df %>% select(-c(col1, col2))

# Rename columns
# df %>% rename(new_name = old_name)

# Join dataframes on column
# df1 %>% merge(df2, by = "common.col", all.x = TRUE)

# Generate fake numbers
# sample.int(100, 10) # For ten integers ranging from 1-100

#---- Load Packages ----
library(tidyverse)
library(bigrquery)
library(googleCloudStorageR)
library(DBI)

#---- Load Data ----
# Authenticate GCP / BQ
bq_auth(Sys.getenv('gcp_credentials'))
gcs_auth(Sys.getenv('gcp_credentials'))

# Connect to project
bqcon <- dbConnect(
  bigrquery::bigquery(),
  project = "freightwaves-data-factory"
)

# Read in ticker data
days_of_data <- 140
where_clause <- "index_id = 9" # Use 'TRUE' to select all
ticker_data  <- dbGetQuery(bqcon, paste0("SELECT *
                                          FROM `freightwaves-data-factory.index_time_series.indx_index_data`
                                            AS index_data
                                          JOIN freightwaves-data-factory.index_time_series.indx_index_definition
                                            AS index_definition
                                          ON index_data.index_id = index_definition.id
                                          WHERE data_timestamp BETWEEN TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL ", days_of_data, " DAY))
                                                                   AND TIMESTAMP(CURRENT_DATE())
                                          AND ",  where_clause))

#---- Exploratory Data Analysis ----
# Analyze metadata
str(ticker_data)
summary(ticker_data)

# Get rid of useless columns and turn appropriate values into factors
ticker_data <- ticker_data %>% 
  select(-c(id_1, frequency, unit_type, map_display, chart_display, precision, invert_color, has_past_data, has_future_data, product, periodicity, display_unit_type)) %>%
  mutate(ticker_index        = factor(paste(index_id, granularity_item_id, sep = "_")),
         id                  = factor(id),
         # granularity_item_id = factor(granularity_item_id), # Keeping this as an int makes subsetting easy
         index_id            = factor(index_id),
         index_name          = factor(index_name),
         ticker              = factor(ticker),
         data_source         = factor(data_source),
         elt_id              = factor(elt_id))
str(ticker_data)

# What columns have the largest amount of NA values?
sapply(ticker_data, function(x) sum(is.na(x)))

# How many distinct tickers are there?
n_distinct(ticker_data$ticker_index)

# How many records of data are there for each ticker?
ticker_occurrence <- ticker_data %>%
  count(ticker_index, name = 'count')

# Find the top tickers by merging the ticker occurrences and the ticker dataframe 
top_tickers <- ticker_data %>%
  merge(ticker_occurrence, by = "ticker_index", all.x = TRUE) %>%
  filter(count > 100)

ticker_plot <- ggplot(ticker_data[ticker_data$granularity_item_id == 1, ], aes(data_timestamp, data_value, color = ticker_index)) +
  geom_line() +
  theme(legend.position = 'None')
ticker_plot

#---- Anomaly Detection for One Ticker----
# Add fake value(s) for testing purposes
impute_fakes <- function(df, index, multiplication_factor) {
  
  # Multiply the value at the given index by a given factor
  df[index, "data_value"] <- df[index, "data_value"] * multiplication_factor
  
  return(df)
}

# Detect if the newest record is anomalous
detect_anomaly <- function(df) {
  
  # Load proper packages
  library(tibbletime)
  library(anomalize)
  library(timetk)
  
  # Preprocess dataframe and then perform anomaly calculations
  anomaly_df <- df %>% 
    select(data_timestamp, data_value) %>% 
    arrange(data_timestamp) %>% 
    as_tibble() %>%
    time_decompose(data_value, merge = TRUE) %>%
    anomalize(remainder) %>%
    time_recompose() %>%
    rowwise() %>% 
    mutate(buffer_zero   = mean(remainder_l1, remainder_l2),
           buffer_radius = abs(remainder_l2 - buffer_zero),
           score = (abs(remainder) - buffer_zero) / buffer_radius)
  
  score  <- anomaly_df[nrow(anomaly_df), 'score']
  result <- anomaly_df[nrow(anomaly_df), 'anomaly']
  
  return(c(score, result))
  
  # Old method of returning
  # if(anomaly_df[nrow(anomaly_df), 'anomaly'] == 'Yes') {
  #   return(c(anomaly_df[nrow(anomaly_df), 'score'], TRUE))
  # }
  # else {
  #   return(c(anomaly_df[nrow(anomaly_df), 'score'], FALSE))
  # }
  
}

# Impute test value and then run anomaly detection on that record
otri_usa_test_ticker_data <- ticker_data[ticker_data$ticker_index == '9_1', ] %>%
  arrange(data_timestamp) %>% 
  impute_fakes(nrow(.), 0)

detect_anomaly(otri_usa_test_ticker_data)

#---- Anomaly Detection for Multiple Tickers ----
# Ticker test dataset
tickers_test <- ticker_data[ticker_data$granularity_item_id < 160, ] %>% 
  arrange(granularity_item_id, data_timestamp)

# Apply random fakes
ticker_test_count <- n_distinct(tickers_test$granularity_item_id)
random_indices    <- 140 * sort(sample.int(ticker_test_count, sample.int(ticker_test_count, 1)))
tickers_test <- impute_fakes(tickers_test, random_indices, 0)

# Get a dataframe of every ticker
tickers_ID <- tickers_test %>% distinct(ticker_index)

# Function to detect anomalies of a given ticker
master_anomaly_detector <- function(df, ticker, anomaly_df) {
  
  anomaly     <- detect_anomaly(df[df$ticker_index == ticker, ])
  result      <- data.frame(ticker = ticker, score = anomaly$score, anomaly = anomaly[2])
  anomaly_df <<- rbind(anomaly_df, result) # <<- alters global variable
  
}

# Create empty dataframe and then run the anomaly detector on every ticker
anomaly_df <- data.frame(ticker = NULL, score = NULL, anomaly = NULL)
# ptm <- proc.time()
apply(tickers_ID, 1, function(x) master_anomaly_detector(tickers_test, x, anomaly_df))
# proc.time() - ptm

