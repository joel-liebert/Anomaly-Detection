-- TODO: index 185 has 0s that show up in division and halt the query

declare eval_date timestamp;
declare data_periods  int64;
declare ind_id        int64;
declare stddev_lim  float64;
declare day_hours     int64;

set eval_date    = timestamp(current_date);
set data_periods = 28;
set ind_id       = 185;
set stddev_lim   = 4;
set day_hours    = 24;

with row_data as (
    select
      data_timestamp
      ,lag(data_timestamp)
        over standard
        as last_date
      ,last_value(data_timestamp)
        over (standard
              range between unbounded preceding and unbounded following)
        as most_recent_date
      ,row_number()
        over (partition by index_id, granularity_item_id
              order by data_timestamp desc) 
        as row_num
      ,index_id
      ,granularity_item_id
      ,round(data_value, 4)
        as data_value
      ,round(lag(data_value, 1)
        over standard, 4)
        as prev_val
      ,round(avg(data_value)
        over (standard
              rows between 7 preceding and 1 preceding), 4)
        as seven_period_avg
    from `freightwaves-data-factory.index_time_series.indx_index_data`
    where data_timestamp <= eval_date
      and index_id        < ind_id
    window standard as (
      partition by index_id, granularity_item_id
      order by data_timestamp
    )
  )
  ,detrended_data as (
    select
      *
      ,abs(round(data_value - prev_val, 4))
        as detrended_val
      ,data_timestamp - last_date
        as time_diff
      ,max(row_num)
        over (partition by index_id, granularity_item_id
              order by data_timestamp
              range between unbounded preceding and unbounded following)
        as max_row_num
    from row_data
    where row_num <= data_periods
  )
  ,stats_data as (
    select
      *
      ,round(avg(detrended_val)
        over standard, 4)
        as detrended_avg
      ,round(stddev_samp(detrended_val)
        over standard, 4)
        as detrended_std_dev
      ,avg(time_diff)
        over standard
        as avg_time_diff
    from detrended_data
    where max_row_num >= data_periods
    window standard as (
      partition by index_id, granularity_item_id
      order by data_timestamp
      range between unbounded preceding and unbounded following
    )
  )
  ,stddev_data as (
    select
      *
    ,extract(hour from avg_time_diff)
      as avg_hrs_diff
    ,round(abs(detrended_val - detrended_avg) / detrended_std_dev, 4)
      as stddev_away
    from stats_data
  )

select
  -- *
  case when ticker_data.stddev_away >= stddev_lim
    then 1
    else 0
    end as anomaly
  ,ticker_data.data_value
  ,ticker_data.prev_val
  ,ticker_data.seven_period_avg
  ,ticker_data.detrended_val
  ,ticker_data.detrended_avg
  ,ticker_data.detrended_std_dev
  ,ticker_data.stddev_away
  ,ticker_data.data_timestamp
  -- ,row_num
  ,info_data.ticker
  ,info_data.index_name
    as granularity
  ,gran_data.Description
    as gran_desc
  ,info_data.frequency
  ,avg_hrs_diff / day_hours
    as avg_days_bw_records
  ,info_data.unit_type
  ,info_data.description
    as info_desc
  ,ticker_data.index_id
  ,ticker_data.granularity_item_id
  ,gran_data.granularity1
from stddev_data as ticker_data
join `freightwaves-data-factory.index_time_series.indx_granularity_item` as gran_data
  on ticker_data.granularity_item_id = gran_data.id
join `freightwaves-data-factory.index_time_series.indx_index_definition` as info_data
  on ticker_data.index_id = info_data.id
where data_timestamp = most_recent_date
-- where data_timestamp = eval_date - 1
order by
  anomaly desc
  ,ticker_data.stddev_away desc
  ,ticker_data.data_timestamp desc
  ,ticker_data.granularity_item_id
