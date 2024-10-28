from google.cloud import bigquery

class BQClient:
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client()

    def create_table(self, schema, table_id, table_description=None, table_labels=None):
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        if table_description is not None:
            table.description = table_description
        if table_labels is not None:
            table.labels = table_labels
        table = self.client.create_table(table)  # API request
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        return table


    def create_schema(schema_json):
        schema = []
        for field in schema_json:
            schema.append(bigquery.SchemaField(field["name"], field["type"], mode=field["mode"]))
        return schema


    def insert_rows(self, table_id, json_data):
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        errors = self.client.insert_rows_json(table_ref, json_data)
        if errors:
            print(f"Errors occurred: {errors}")
        else:
            print(f"Data successfully loaded into {table_id}")


# if __name__ == "__main__":
#     project_id = "your-project-id"
#     dataset_id = "your-dataset-id"
#     table_name = "your-table-name"
#     bq_client = BQClient(project_id=project_id, dataset_id=dataset_id)
#     bq_client.create_table(schema, table_id=table_name)
