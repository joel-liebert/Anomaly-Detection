source("C:/Users/jliebert/Documents/projects/Anomaly-Detection/func.R")
# detect_anomaly(ticker_data, '2_1')

input_df <- ticker_data
ticker_index <- '2_1'

df <- input_df[input_df$ticker_index == ticker_index, ] %>% 
  select(data_timestamp, data_value) %>% 
  arrange(data_timestamp) %>% 
  as_tibble() %>%
  time_decompose(data_value, merge = TRUE, message = FALSE)
%>%
  anomalize(remainder) %>%
  time_recompose() %>%
  rowwise() %>% 
  mutate(buffer_zero   = mean(remainder_l1, remainder_l2),
         buffer_radius = abs(remainder_l2 - buffer_zero),
         score = (abs(remainder) - buffer_zero) / buffer_radius)




test2_1 <- ticker_data %>%
  filter(ticker_index == '2_1')
test3_1 <- ticker_data %>%
  filter(ticker_index == '3_1')


test2_1 <- test2_1 %>%
  mutate(date_seq = match(data_timestamp, unique(data_timestamp)))

test2_1 <- test2_1 %>%
  select(-c(data_timestamp)) %>%
  rename(data_timestamp = date_seq)

detect_anomaly(test2_1, '2_1')
