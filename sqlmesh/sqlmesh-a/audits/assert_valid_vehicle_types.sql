AUDIT (
  name assert_valid_vehicle_types,
  dialect spark
);

SELECT 
  inventory_id,
  event_timestamp,
  vehicle_type
FROM @this_model
WHERE vehicle_type NOT IN ('USED_CAR', 'COMMISSION_CAR')
  AND vehicle_type IS NOT NULL
