# Only available for paid users.

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

warehouse_id = "7474656153926720"

w.warehouses.edit(warehouse_id, auto_stop_mins=1)