declare eval_date        date; -- the date to be evaluated
declare data_periods    int64; -- the number of days/weeks/months included in the calculations
declare ind_id          int64; -- the largest index to be evaluated
declare stddev_lim    float64; -- the threshold for standard deviations
declare value_rep_lim float64; -- the threshold for consecutive value repetitions
declare update_lim    float64; -- the threshold multiplier for days since last update
declare day_hours       int64; -- the number of hours in a day

set eval_date     = current_date;
set data_periods  = 28;
set ind_id        = 10000;
set stddev_lim    = 4.5;
set value_rep_lim = 3;
set update_lim    = 1;
set day_hours     = 24;

with row_data as (
    select
      data_timestamp
        as date_recorded
      ,lag(data_timestamp)
        over standard
        as last_date
      ,current_date
        as run_date
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
        as value
      ,round(lag(data_value, 1)
        over standard, 4)
        as previous_value
      ,round(avg(data_value)
        over (standard
              rows between 7 preceding and 1 preceding), 4)
        as previous_seven_period_avg
    from `freightwaves-data-factory.index_time_series.indx_index_data`
    where data_timestamp <= timestamp(eval_date)
      and index_id        < ind_id
    window standard as (
      partition by index_id, granularity_item_id
      order by data_timestamp
    )
  )
  ,detrended_data as (
    select
      *
      ,abs(round(value - previous_value, 4))
        as absolute_one_period_difference
      ,date_recorded - last_date
        as time_diff
      -- ,max(row_num)
      --   over (partition by index_id, granularity_item_id
      --         order by data_timestamp
      --         range between unbounded preceding and unbounded following)
      --   as max_row_num
    from row_data
    where row_num <= data_periods
  )
  ,stats_data as (
    select
      *
      ,round(avg(absolute_one_period_difference)
        over standard, 4)
        as average
      ,round(stddev_samp(absolute_one_period_difference)
        over standard, 4)
        as standard_deviation
      ,avg(time_diff)
        over standard
        as avg_days_bw_periods
      ,case when absolute_one_period_difference = 0
        then 1
        else 0
        end as repeated_values
      ,case when absolute_one_period_difference = 0
        then 0
        else 1
        end as reset_count
    from detrended_data
    -- where max_row_num >= data_periods
    window standard as (
      partition by index_id, granularity_item_id
      order by date_recorded
      range between unbounded preceding and unbounded following
    )
  )
  ,stddev_data as (
    select
      *
      ,extract(hour from avg_days_bw_periods) / day_hours
        as avg_days_bw_data
      ,extract(hour from (timestamp(run_date) - date_recorded)) / day_hours
        as days_since_last_update
      ,case when standard_deviation != 0
        then round(abs(absolute_one_period_difference - average) / standard_deviation, 4)
        else 0
        end as absolute_standard_deviations_from_avg
      -- ,sum(repeated_vals)
      --   over (partition by index_id, granularity_item_id
      --         order by data_timestamp)
      --   as rep_running_total
      ,sum(reset_count)
        over (partition by index_id, granularity_item_id
              order by date_recorded)
        as reset_reps_sum
    from stats_data
  )
  ,repeated_data as (
    select
      *
      ,sum(
        case when reset_count = 1
        then 1
        else repeated_values
        end
        ) over (partition by index_id, granularity_item_id, reset_reps_sum
                order by date_recorded)
        as data_repetitions
    from stddev_data
  )
  ,flag_data as (
    select
      *
      ,case when absolute_standard_deviations_from_avg >= stddev_lim
        then 1
        else 0
        end as standard_deviation_flag
      ,case when data_repetitions >= value_rep_lim
        then 1
        else 0
        end as data_repetitions_flag
      ,case when days_since_last_update > update_lim * avg_days_bw_data
        then 1
        else 0
        end as days_since_last_update_flag
      from repeated_data
  )

select
  case when
      ticker_data.standard_deviation_flag        = 1
      or ticker_data.data_repetitions_flag       = 1
      or ticker_data.days_since_last_update_flag = 1
    then 1
    else 0
    end as anomaly
  ,ticker_data.standard_deviation_flag
  ,ticker_data.data_repetitions_flag
  ,ticker_data.days_since_last_update_flag
  ,ticker_data.value
  ,ticker_data.previous_value
  ,ticker_data.previous_seven_period_avg
  ,ticker_data.absolute_one_period_difference
  ,ticker_data.average
  ,ticker_data.standard_deviation
  ,ticker_data.absolute_standard_deviations_from_avg
  ,case when ticker_data.value - ticker_data.previous_value < 0
    then -1 * ticker_data.absolute_standard_deviations_from_avg
    else      ticker_data.absolute_standard_deviations_from_avg
    end as standard_deviations_from_avg
  ,ticker_data.data_repetitions
  ,ticker_data.date_recorded
  ,ticker_data.run_date
  ,info_data.index_name
    as ticker
  ,gran_data.Description
    as granularity
  ,info_data.description
    as ticker_info
  ,info_data.ticker
    as ticker_code
  ,gran_data.granularity1
    as granularity_code
  ,info_data.frequency
    as data_pull_frequency
  ,ticker_data.avg_days_bw_data
  ,ticker_data.days_since_last_update
  ,info_data.unit_type
  ,ticker_data.index_id
  ,ticker_data.granularity_item_id
    as granularity_id
  -- *,
  -- ,last_date
  -- ,most_recent_date
  -- ,row_num
  -- ,time_diff
  -- ,avg_time_diff
  -- ,max_row_num
  -- ,repeated_vals
  -- ,reset_reps
  -- ,rep_running_total
  -- ,reset_reps_sum
  -- ,granularity2
  -- ,ShapeFile
  -- ,map_display
  -- ,chart_display
  -- ,precision
  -- ,invert_color
  -- ,has_past_data
  -- ,has_future_data
  -- ,product
  -- ,data_source
  -- ,elt_id
  -- ,documentation_url
  -- ,periodicity
  -- ,display_unit_type
from flag_data as ticker_data
join `freightwaves-data-factory.index_time_series.indx_granularity_item` as gran_data
  on ticker_data.granularity_item_id = gran_data.id
join `freightwaves-data-factory.index_time_series.indx_index_definition` as info_data
  on ticker_data.index_id = info_data.id
-- where date_recorded = most_recent_date
where date_recorded = timestamp(eval_date)
order by
  ticker_data.absolute_standard_deviations_from_avg desc
  ,ticker_data.date_recorded desc
  ,ticker_data.granularity_item_id
  -- ticker_data.granularity_item_id
  -- ,ticker_data.date_recorded desc
