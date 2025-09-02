{{
  config(
    materialized='table',
    comment='Daily inquiry metrics aggregated by date, vehicle, and inquiry characteristics'
  )
}}

with inventory_data as (
    select *
    from {{ ref('agg_inventory_reporting_demo') }}
),

date_exploded as (
    select 
        inventory_id,
        region,
        market,
        country,
        vehicle_condition,
        model_series,
        vehicle_type,
        engine_type,
        dealer_name,
        online_orderable,
        porsche_approved,
        -- Generate array of dates between valid_from and valid_to
        stock_date,
        datediff(day, date(valid_from), stock_date) as stock_days
    from inventory_data
    lateral view outer explode(
        sequence(
            date(valid_from),
            date(valid_to),
            interval 1 day
        )
    ) as stock_date
),

final_aggregated as (
    select 
        stock_date,
        region,
        market,
        country,
        vehicle_condition,
        model_series,
        vehicle_type,
        engine_type,
        dealer_name,
        count(*) as published_net_stock_count,
        count(*) as net_stock_count,
        sum(case when online_orderable then 1 else 0 end) as online_orderable_count,
        sum(case when porsche_approved then 1 else 0 end) as approved_count,
        sum(stock_days) as stock_days
    from date_exploded
    group by 
        stock_date,
        region,
        market,
        country,
        vehicle_condition,
        model_series,
        vehicle_type,
        engine_type,
        dealer_name
)

select * from final_aggregated
