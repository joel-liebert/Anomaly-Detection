declare day timestamp;
declare ind_id int64;
set day = '2022-06-01';
set ind_id = 3;

with ts_data as (
  select 
    data_timestamp
    ,index_id
    ,granularity_item_id
    ,round(data_value, 3)
      as data_value
    ,round(avg(data_value)
      over standard, 3)
      as mean
    ,round(stddev_samp(data_value)
      over standard, 3)
      as standard_deviation
  from `freightwaves-data-factory.index_time_series.indx_index_data`
  where index_id = ind_id
    and data_timestamp >= day
  window standard as (
    partition by granularity_item_id
    order by data_timestamp
    range between unbounded preceding and unbounded following
  )
  order by granularity_item_id, data_timestamp
)

select
  *,
  round(abs(data_value - mean) / ts_data.standard_deviation, 3)
    as stddev_away
from ts_data