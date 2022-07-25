
from google.cloud import bigquery

client = bigquery.Client()
table_id = "mda-data-warehouse-324819.mda_demo"
schema = [
    bigquery.SchemaField("order_SK", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)
