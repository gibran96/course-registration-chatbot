from src.bq_utils import BQClient

bq_client = BQClient(project_id="your-project-id", dataset_id="your-dataset-id")

trace_table_schema = {
    'table_name': 'TRACE_DATA',
    'schema': {
        'fields': [
            {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'crn', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'response', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'instructor', 'type': 'STRING', 'mode': 'REQUIRED'}
        ]
    }
}


if __name__ == "__main__":
    project_id = "your-project-id"
    dataset_id = "your-dataset-id"
    table_name = "your-table-name"

    bq_client.create_table(schema=bq_client.create_schema(trace_table_schema['schema']['fields']), 
                        table_id=trace_table_schema['table_name'])