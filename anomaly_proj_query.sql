declare eval_date timestamp;
declare data_periods  int64;
declare ind_id        int64;
declare stddev_lim  float64;

set eval_date    = timestamp(current_date-1);
set data_periods = 28;
set ind_id       = 9;
set stddev_lim   = 4.5;

with row_data as (
    select
      data_timestamp
    ,last_value(data_timestamp)
      over (standard
            range between unbounded preceding and unbounded following)
      as most_recent_date
    ,row_number()
      over (partition by granularity_item_id
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
    where index_id        = ind_id
      and data_timestamp <= eval_date
    window standard as (
      partition by granularity_item_id
      order by data_timestamp
    )
  )
  ,detrended_data as (
    select
      *
      ,abs(round(data_value - prev_val, 4))
        as detrended_val
    from row_data
    where row_num <= data_periods
  )
  ,stats_data as (
    select
      *
      ,round(avg(detrended_val)
        over standard, 4)
        as detrended_mean
      ,round(stddev_samp(detrended_val)
        over standard, 4)
        as detrended_std_dev
    from detrended_data
    window standard as (
      partition by granularity_item_id
      order by data_timestamp
      range between unbounded preceding and unbounded following
    )
  )
  ,stddev_data as (
    select
      *
      ,round(abs(detrended_val - detrended_mean) / detrended_std_dev, 4)
        as stddev_away
    from stats_data
  )

select
  -- *
  data_timestamp
  -- ,most_recent_date
  -- ,row_num
  -- ,index_id
  ,granularity_item_id
  ,data_value
  ,prev_val
  ,seven_period_avg
  ,detrended_val
  ,detrended_mean
  ,detrended_std_dev
  ,stddev_away
  ,case when stddev_away > stddev_lim
    then 1
    else 0
    end as anomaly
from stddev_data
where data_timestamp = most_recent_date
-- where data_timestamp = eval_date
order by
  anomaly desc
  -- ,data_timestamp desc
  ,stddev_away desc
  ,granularity_item_id asc
