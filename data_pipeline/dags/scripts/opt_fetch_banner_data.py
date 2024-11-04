import ast
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import pandas as pd

from scripts.fetch_banner_data import get_course_description, get_course_prerequisites, get_faculty_info

# Split course_list_df into smaller batches
def split_course_list_df(course_list_df, batch_size=10):
    """Yield batches of the DataFrame."""
    for i in range(0, len(course_list_df), batch_size):
        yield course_list_df.iloc[i:i + batch_size]

# Merge results from parallel processing back into a single DataFrame
def merge_course_data_df(batch_results):
    """Concatenate batch results back into a single DataFrame."""
    return pd.concat(batch_results, ignore_index=True)

# Process faculty info in parallel for a batch of courses
def process_faculty_info_batch_df(cookie_output, course_batch_df):
    """Process faculty info for a batch of courses."""
    return get_faculty_info(cookie_output, course_batch_df)

# Process course descriptions in parallel for a batch of courses
def process_description_batch_df(cookie_output, course_batch_df):
    """Process course descriptions for a batch of courses."""
    return get_course_description(cookie_output, course_batch_df)

# Process prerequisites in parallel for a batch of courses
def process_prerequisites_batch_df(cookie_output, course_batch_df):
    """Process prerequisites for a batch of courses."""
    return get_course_prerequisites(cookie_output, course_batch_df)

def parallel_process_with_threads_df(process_func, cookie_output, course_list_df, max_workers=5):
    """
    Generic function to process DataFrame batches using ThreadPoolExecutor.
    """
    try:
        batches = list(split_course_list_df(course_list_df))
        results = []

        process_batch = partial(process_func, cookie_output)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(process_batch, batch): batch
                for batch in batches
            }

            for future in as_completed(future_to_batch):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    batch = future_to_batch[future]
                    logging.error(f"Batch processing failed {batch.index}: {str(e)}")

        # Ensure there are objects to concatenate
        if results:
            return merge_course_data_df(results)
        else:
            raise ValueError("No valid DataFrames to concatenate")

    except Exception as e:
        logging.error(f"Error in parallel processing with DataFrames: {e}")
        raise

# DAG tasks for faculty info
def parallel_faculty_info(**context):
    try:
        cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
        course_list_df = context['ti'].xcom_pull(task_ids='get_course_list_task', key='course_list_df')

        course_list_df = course_list_df.copy()  # Avoid SettingWithCopyWarning
        logging.info(f"Length of course list DataFrame: {len(course_list_df)}")
        
        results_df = parallel_process_with_threads_df(
            process_faculty_info_batch_df,
            cookie_output,
            course_list_df
        )
        logging.info(f"Length of results: {len(results_df)}")
        
        if results_df.empty:
            raise ValueError("Error in parallel_faculty_info")
        
        context['ti'].xcom_push(key='course_list_df', value=results_df)
    
    except Exception as e:
        logging.error(f"Error in parallel_faculty_info: {str(e)}")
        raise ValueError(f"Error in parallel_faculty_info: {str(e)}") from e

# DAG tasks for course description
def parallel_course_description(**context):
    try:
        cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
        course_list_df = context['ti'].xcom_pull(task_ids='get_faculty_info_task', key='course_list_df')
        
        course_list_df = course_list_df.copy()  # Avoid SettingWithCopyWarning
        logging.info(f"Length of course list DataFrame: {len(course_list_df)}")
        
        results_df = parallel_process_with_threads_df(
            process_description_batch_df,
            cookie_output,
            course_list_df
        )
        logging.info(f"Length of results: {len(results_df)}")
        
        if not results_df:
            raise ValueError("Error in parallel_course_description")
        
        context['ti'].xcom_push(key='course_list_df', value=results_df)
    except Exception as e:
        logging.error(f"Error in parallel_course_description: {e}")
        raise
    
# DAG tasks for prerequisites
def parallel_prerequisites(**context):
    try:
        cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
        course_list_df = context['ti'].xcom_pull(task_ids='get_course_description_task', key='course_list_df')
        
        course_list_df = course_list_df.copy()  # Avoid SettingWithCopyWarning
        logging.info(f"Length of course list DataFrame: {len(course_list_df)}")
        
        results_df = parallel_process_with_threads_df(
            process_prerequisites_batch_df,
            cookie_output,
            course_list_df
        )
        logging.info(f"Length of results: {len(results_df)}")
        
        if results_df.empty:
            raise ValueError("Error in parallel_prerequisites")
        
        context['ti'].xcom_push(key='course_list_df', value=results_df)
    except Exception as e:
        logging.error(f"Error in parallel_prerequisites: {e}")
        raise
