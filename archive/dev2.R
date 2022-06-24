install.packages("devtools")
devtools::install_github("twitter/AnomalyDetection")
library(AnomalyDetection)

data(raw_data)

res <- AnomalyDetectionTs(test2_1[, names(test2_1) %in% c("data_timestamp", "data_value")], max_anoms=0.02, direction='both', plot=TRUE)

res$anoms

