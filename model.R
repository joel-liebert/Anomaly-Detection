#---- Useful Code ----

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
days_ago    <- 7
ticker_data <- dbGetQuery(bqcon, paste0("SELECT *
                                          FROM `freightwaves-data-factory.index_time_series.indx_index_data`
                                            AS index_data
                                          JOIN freightwaves-data-factory.index_time_series.indx_index_definition
                                            AS index_definition
                                          ON index_data.index_id = index_definition.id
                                          WHERE data_timestamp BETWEEN TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL ", days_ago, " DAY))
                                                                   AND TIMESTAMP(CURRENT_DATE());"))
