
# TBD:
 * liquid clustering (databricks features)  
   💡 could not find any hints (probably not supported - maybe custom_materialization)
 * GRANT permission on model  
   💡 cdp_inventory_reporting_demo.sql
 * merge (INCREMENTAL_BY_UNIQUE_KEY)  
   💡 sdp_inventory_vehicles_preview_incremental_key
 * blue green deployment  
   💡 make changes and deploy to own "environment" (sqlmesh plan my_dev -> schema "test__my_dev" instead of "test")  
   💡 only executes actual changes (thanks to virtual layers)  
   💡 compare sqlmesh table_diff prod:my_dev test.cdp_inventory_reporting_demo  
 * orchestration (databricks workflows?)  
   💡 not possible  
   💡 dedicated resource necessary running "sqlmesh run"
 * multi repository support (orchestration)  
   💡 sqlmesh -p sqlmesh-a -p sqlmesh-b plan
 * monitoring  
   💡 central sqlmesh process which orchestrates everything  
   💡 how does monitoring on a product team level work?
 * postgres state backend

# Community discussion:
https://smallbigdata.substack.com/p/what-if-dbt-and-sqlmesh-were-weapons
https://www.reddit.com/r/dataengineering/comments/1ik3i6e/dbt_vs_sqlmesh/
