{{
  config(
    materialized='table',
    comment='Source-aligned data product for inventory vehicles from raw preview data'
  )
}}

with raw_data as (
    select *
    from {{ source('inventory_vehicles', 'raw_inventory_vehicles_demo') }}
),

-- Extract the vehicle object from the JSON wrapper
vehicle_extracted as (
    select 
        timestamp,
        processed_at,
        get_json_object(value, '$.vehicle') as vehicle_json
    from raw_data
),

-- Parse the JSON vehicle object to extract vehicle inventory data
parsed_data as (
    select
        -- Extract JSON fields from the unwrapped vehicle object
        timestamp as event_timestamp,
        cast(get_json_object(vehicle_json, '$.arrivalDate') as date) as arrival_date,
        cast(get_json_object(vehicle_json, '$.salesDate') as date) as sales_date,
        get_json_object(vehicle_json, '$.id') as inventory_id,
        get_json_object(vehicle_json, '$.country') as country,
        get_json_object(vehicle_json, '$.conditionType') as vehicle_condition,
        get_json_object(vehicle_json, '$.vehicleType') as vehicle_type,
        -- Extract dealer name from the nested dealership object
        -- Note: Simplified extraction as dbt/SQL doesn't have exact equivalent of map_values().getItem(0)
        get_json_object(vehicle_json, '$.dealership.displayName') as dealer_name,
        -- Handle boolean fields with coalesce for null values
        coalesce(
            cast(get_json_object(vehicle_json, '$.onlineOrderingEnabled') as boolean),
            false
        ) as online_orderable,
        cast(get_json_object(vehicle_json, '$.porscheApproved') as boolean) as porsche_approved,
        processed_at
    from vehicle_extracted
    where get_json_object(vehicle_json, '$.id') is not null  -- Remove rows where inventory_id is null
),

-- Add deduplication logic - deduplicate by inventory_id
-- Keep the most recent record based on processed_at for each inventory_id and event_timestamp combination
deduplicated_data as (
    select 
        event_timestamp,
        arrival_date,
        sales_date,
        inventory_id,
        country,
        vehicle_condition,
        vehicle_type,
        dealer_name,
        online_orderable,
        porsche_approved,
        processed_at,
        row_number() over (
            partition by inventory_id, event_timestamp 
            order by processed_at desc
        ) as row_num
    from parsed_data
)

select 
    event_timestamp,
    arrival_date,
    sales_date,
    inventory_id,
    country,
    vehicle_condition,
    vehicle_type,
    dealer_name,
    online_orderable,
    porsche_approved,
    processed_at
from deduplicated_data
where row_num = 1
