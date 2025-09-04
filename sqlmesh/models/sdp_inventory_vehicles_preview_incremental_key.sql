MODEL (
  name test.sdp_inventory_vehicles_preview_incremental_key,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key inventory_id
  ),
  start '2025-08-27',
  cron '@daily',
  description 'Source aligned data product for inventory vehicles - Incremental Version.'
);

/*
Source aligned data product for inventory vehicles - Incremental Version.
This model transforms raw inventory vehicle data by:
1. Parsing JSON vehicle snapshot data
2. Extracting key fields (inventory_id)
3. Deduplicating records by keeping the latest event per inventory_id
4. Incrementally loading based on event_timestamp
*/

with parsed_data as (
    select
        timestamp as event_timestamp,
        get_json_object(get_json_object(value, '$.vehicleSnapshot'), '$.id') as inventory_id,
        get_json_object(get_json_object(value, '$.vehicleSnapshot'), '$.removalDateTime') as removal_date_time,
        get_json_object(get_json_object(value, '$.vehicleSnapshot'), '$.creationDateTime') as creation_date_time
    from car_sales_prod.inventory_vehicles_prod.raw_inventory_vehicles_preview
    where get_json_object(get_json_object(value, '$.vehicleSnapshot'), '$.id') is not null
      and timestamp between @start_date and @end_date
),

deduplicated_data as (
    select
        event_timestamp,
        inventory_id,
        removal_date_time,
        creation_date_time,
        row_number() over (
            partition by inventory_id 
            order by event_timestamp desc
        ) as row_number
    from parsed_data
)

select
    event_timestamp,
    inventory_id,
    removal_date_time,
    creation_date_time
from deduplicated_data
where row_number = 1
