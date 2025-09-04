MODEL (
  name test_b.sdp_inventory_vehicles_preview,
  kind FULL,
  cron '@daily',
  grain (inventory_id),
  description 'dependency for sdp_inventory_vehicles',
);

select * from test.sdp_inventory_vehicles_preview
