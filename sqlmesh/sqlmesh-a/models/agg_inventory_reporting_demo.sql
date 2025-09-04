MODEL (
  name test.agg_inventory_reporting_demo,
  kind FULL,
  cron '@daily',
  description 'Aggregated vehicle inventory data for reporting',
  grain (inventory_id)
);

with inventory_events as (
    select 
        inventory_id,
        event_timestamp,
        country,
        vehicle_condition,
        vehicle_type,
        dealer_name,
        online_orderable,
        porsche_approved,
        arrival_date,
        sales_date,
        processed_at
    from test.sdp_inventory_vehicles_demo
),

market_data as (
    select *
    from car_sales_dev.master_data.markets
),

vds_data as (
    select 
        inventory_id,
        model_series,
        engine_type
    from car_sales_prod.vds_vehicle_data_prod.sdp_vds_vehicle_data_demo
),

latest_values as (
    select 
        inventory_id,
        event_timestamp,
        -- Use window functions to get latest values for each inventory_id
        first_value(country) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as country,
        first_value(vehicle_condition) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as vehicle_condition,
        first_value(vehicle_type) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as vehicle_type,
        first_value(dealer_name) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as dealer_name,
        first_value(online_orderable) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as online_orderable,
        first_value(porsche_approved) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as porsche_approved,
        first_value(arrival_date) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as arrival_date,
        first_value(sales_date) over (
            partition by inventory_id 
            order by event_timestamp desc 
            rows between unbounded preceding and unbounded following
        ) as sales_date,
        processed_at
    from inventory_events
),

aggregated_inventory as (
    select 
        inventory_id,
        -- Get earliest event timestamp as valid_from (VehicleArrivedAtDealership)
        min(event_timestamp) as valid_from,
        -- Get latest event timestamp as valid_to (VehicleSold/VehicleDelivered)
        max(event_timestamp) as valid_to,
        -- Use any_value to get the latest values (they're all the same due to window function)
        any_value(country) as country,
        any_value(vehicle_condition) as vehicle_condition,
        any_value(vehicle_type) as vehicle_type,
        any_value(dealer_name) as dealer_name,
        any_value(online_orderable) as online_orderable,
        any_value(porsche_approved) as porsche_approved,
        any_value(arrival_date) as arrival_date,
        any_value(sales_date) as sales_date,
        -- Count of events per vehicle
        count(*) as event_count,
        -- Latest processed timestamp
        max(processed_at) as processed_at
    from latest_values
    group by inventory_id
),

final as (
    select 
        agg.inventory_id,
        agg.valid_from,
        agg.valid_to,
        agg.country,
        market.region,
        market.market,
        agg.vehicle_condition,
        vds.model_series,
        agg.vehicle_type,
        vds.engine_type,
        agg.dealer_name,
        agg.online_orderable,
        agg.porsche_approved,
        agg.arrival_date,
        agg.sales_date,
        agg.event_count,
        agg.processed_at
    from aggregated_inventory agg
    left join market_data market on agg.country = market.country
    left join vds_data vds on agg.inventory_id = vds.inventory_id
)

select * from final
