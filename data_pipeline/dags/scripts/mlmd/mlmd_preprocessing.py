import logging
from airflow.models import Variable
from ml_metadata import metadata_store
from ml_metadata.proto import metadata_store_pb2
from datetime import datetime

def setup_mlmd():
    """
    Setup ML Metadata connection and create necessary types.
    """
    # Create connection config for metadata store
    config = metadata_store_pb2.ConnectionConfig()
    config.mysql.host = Variable.get("metadata_db_host")
    config.mysql.port = 3306
    config.mysql.database = Variable.get("metadata_db_name")
    config.mysql.user = Variable.get("metadata_db_user")
    config.mysql.password = Variable.get("metadata_db_password")

    logging.info(f"Connecting to MLMD: {config}")
    

# Initialize the metadata store
    store = metadata_store.MetadataStore(config)

    # Create ArtifactTypes if they don't exist
    try:
        preprocessing_dataset_type = store.get_artifact_type("PreprocessingDataset")
    except:
        preprocessing_dataset_type = metadata_store_pb2.ArtifactType()
        preprocessing_dataset_type.name = "PreprocessingDataset"
        preprocessing_dataset_type.properties["raw_reviews_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["raw_courses_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["processed_reviews_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["processed_courses_count"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["null_responses_removed"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["sensitive_data_flags"] = metadata_store_pb2.INT
        preprocessing_dataset_type.properties["timestamp"] = metadata_store_pb2.STRING
        preprocessing_dataset_type.properties["status"] = metadata_store_pb2.STRING
        preprocessing_dataset_type_id = store.put_artifact_type(preprocessing_dataset_type)

    # Create ExecutionType if it doesn't exist
    try:
        preprocessing_execution_type = store.get_execution_type("Preprocessing")
    except:
        preprocessing_execution_type = metadata_store_pb2.ExecutionType()
        preprocessing_execution_type.name = "Preprocessing"
        preprocessing_execution_type.properties["dag_run_id"] = metadata_store_pb2.STRING
        preprocessing_execution_type.properties["output_path"] = metadata_store_pb2.STRING
        preprocessing_execution_type.properties["start_time"] = metadata_store_pb2.STRING
        preprocessing_execution_type.properties["end_time"] = metadata_store_pb2.STRING
        preprocessing_execution_type_id = store.put_execution_type(preprocessing_execution_type)

    return store

def create_preprocessing_execution(store, **context):
    """
    Create an execution in MLMD for tracking the preprocessing run.
    """
    # Create execution
    execution = metadata_store_pb2.Execution()
    preprocessing_execution_type = store.get_execution_type("Preprocessing")
    execution.type_id = preprocessing_execution_type.id

    execution.properties["dag_run_id"].string_value = context['dag_run'].run_id
    execution.properties["output_path"].string_value = context['dag_run'].conf.get('output_path', '/tmp/processed_data')
    execution.properties["start_time"].string_value = datetime.utcnow().isoformat()
    
    execution_id = store.put_executions([execution])[0]
    return execution_id

def record_preprocessing_metadata(store, execution_id, metadata_values):
    """
    Record preprocessing metadata as artifacts in MLMD.
    """
    # Create artifact
    artifact = metadata_store_pb2.Artifact()
    preprocessing_artifact_type = store.get_artifact_type("PreprocessingDataset")
    artifact.type_id = preprocessing_artifact_type.id

    
    # Set properties
    artifact.properties["raw_reviews_count"].int_value = metadata_values["raw_reviews_count"]
    artifact.properties["raw_courses_count"].int_value = metadata_values["raw_courses_count"]
    artifact.properties["processed_reviews_count"].int_value = metadata_values["processed_reviews_count"]
    artifact.properties["processed_courses_count"].int_value = metadata_values["processed_courses_count"]
    artifact.properties["null_responses_removed"].int_value = metadata_values["null_responses_removed"]
    artifact.properties["sensitive_data_flags"].int_value = metadata_values["sensitive_data_flags"]
    artifact.properties["timestamp"].string_value = metadata_values["timestamp"]
    artifact.properties["status"].string_value = metadata_values["status"]
    
    # Put artifact in store
    artifact_id = store.put_artifacts([artifact])[0]
    
    # Create event to link execution and artifact
    event = metadata_store_pb2.Event()
    event.execution_id = execution_id
    event.artifact_id = artifact_id
    event.type = metadata_store_pb2.Event.Type.OUTPUT
    
    store.put_events([event])
    
    return artifact_id

def get_preprocessing_metadata(store, execution_id):
    """
    Utility function to retrieve preprocessing metadata for a given execution.
    """
    execution = store.get_executions_by_id([execution_id])[0]
    events = store.get_events_by_execution_ids([execution_id])
    
    if events:
        artifact_id = events[0].artifact_id
        artifact = store.get_artifacts_by_id([artifact_id])[0]
        
        metadata = {
            "raw_reviews_count": artifact.properties["raw_reviews_count"].int_value,
            "raw_courses_count": artifact.properties["raw_courses_count"].int_value,
            "processed_reviews_count": artifact.properties["processed_reviews_count"].int_value,
            "processed_courses_count": artifact.properties["processed_courses_count"].int_value,
            "null_responses_removed": artifact.properties["null_responses_removed"].int_value,
            "sensitive_data_flags": artifact.properties["sensitive_data_flags"].int_value,
            "status": artifact.properties["status"].string_value,
            "timestamp": artifact.properties["timestamp"].string_value,
            "dag_run_id": execution.properties["dag_run_id"].string_value,
            "start_time": execution.properties["start_time"].string_value,
            "end_time": execution.properties.get("end_time", metadata_store_pb2.Value()).string_value
        }
        
        return metadata
    return None