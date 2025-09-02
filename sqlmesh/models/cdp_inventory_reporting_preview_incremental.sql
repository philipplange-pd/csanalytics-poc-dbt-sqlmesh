MODEL (
  name test.cdp_inventory_reporting_preview_incremental,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column stock_date
  ),
  cron '@daily',
  grain (stock_date),
  description 'Daily inventory metrics aggregated by date - Incremental Version.'
);

/*
Daily inventory metrics aggregated by date - Incremental Version.
This model transforms inventory vehicle data by:
1. Padding missing removal dates with current timestamp
2. Generating date sequences between creation and removal dates
3. Exploding dates to create daily stock records
4. Aggregating net stock count and stock days by date
5. Incrementally loading based on stock_date
*/

with inventory_data as (
    select
        event_timestamp,
        inventory_id,
        coalesce(removal_date_time, current_timestamp()) as removal_date_time,
        creation_date_time
    from test.sdp_inventory_vehicles_preview_incremental
    where coalesce(removal_date_time, current_timestamp()) > creation_date_time
),

date_sequence_data as (
    select
        inventory_id,
        event_timestamp,
        creation_date_time,
        removal_date_time,
        -- Generate array of dates between creation and removal
        sequence(
            cast(creation_date_time as date),
            cast(removal_date_time as date),
            interval 1 day
        ) as stock_date_array
    from inventory_data
),

exploded_dates as (
    select
        inventory_id,
        event_timestamp,
        creation_date_time,
        removal_date_time,
        explode(stock_date_array) as stock_date
    from date_sequence_data
),

stock_days_calculated as (
    select
        inventory_id,
        stock_date,
        datediff(stock_date, cast(creation_date_time as date)) as stock_days
    from exploded_dates
    where stock_date between @start_date and @end_date
)

select
    stock_date,
    count(*) as net_stock_count,
    sum(stock_days) as stock_days
from stock_days_calculated
group by stock_date
order by stock_date
