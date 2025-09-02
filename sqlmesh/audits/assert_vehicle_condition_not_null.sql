AUDIT (
  name assert_vehicle_condition_not_null,
  dialect spark
);

SELECT 
  inventory_id,
  event_timestamp,
  vehicle_condition
FROM @this_model
WHERE vehicle_condition IS NULL
