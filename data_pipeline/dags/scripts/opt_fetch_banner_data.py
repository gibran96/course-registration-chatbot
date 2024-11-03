import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from scripts.fetch_banner_data import get_course_description, get_course_prerequisites, get_faculty_info

# Helper functions for parallel processing
def split_course_list(course_list, batch_size=10):
    """Split course list into smaller batches"""
    course_items = list(course_list.items())
    for i in range(0, len(course_items), batch_size):
        batch = dict(course_items[i:i + batch_size])
        yield batch

# Merge results from parallel processing
def merge_course_data(batch_results):
    """Merge results from parallel processing back into a single dictionary"""
    merged_data = {}
    for batch in batch_results:
        if batch:
            if isinstance(batch, str):
                try:
                    batch_dict = json.loads(batch)
                except json.JSONDecodeError:
                    logging.error(f"Failed to decode batch: {batch}")
                    continue
            else:
                batch_dict = batch
            merged_data.update(batch_dict)
    return merged_data

# Process faculty info in parallel
def process_faculty_info_batch(cookie_output, course_batch):
    """Process faculty info for a batch of courses."""
    try:
        # Ensure JSON strings are parsed into dictionaries
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        if isinstance(course_batch, str):
            course_batch = json.loads(course_batch)

        # Call the faculty info processing function
        return get_faculty_info(json.dumps(cookie_output), json.dumps(course_batch))
    
    except (TypeError, ValueError) as e:
        logging.error(f"Error processing faculty info batch: {e}")
        return None

# Process course descriptions in parallel
def process_description_batch(cookie_output, course_batch):
    """Process course descriptions for a batch of courses"""
    try:
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        if isinstance(course_batch, str):
            course_batch = json.loads(course_batch)
        return get_course_description(json.dumps(cookie_output), json.dumps(course_batch))
    except Exception as e:
        logging.error(f"Error processing course description batch: {e}")

# Process prerequisites in parallel
def process_prerequisites_batch(cookie_output, course_batch):
    """Process prerequisites for a batch of courses"""
    try:
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        if isinstance(course_batch, str):
            course_batch = json.loads(course_batch)
        return get_course_prerequisites(json.dumps(cookie_output), json.dumps(course_batch))
    except Exception as e:
        logging.error(f"Error processing prerequisites batch: {e}")

# Generic function for parallel processing
def parallel_process_with_threads(process_func, cookie_output, course_list, max_workers=5):
    """
    Generic function to process batches using ThreadPoolExecutor
    """
    try:
        # Ensure course_list is a dictionary
        if isinstance(course_list, str):
            course_list = json.loads(course_list)
            
        batches = list(split_course_list(course_list))
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
                    if result:
                        results.append(result)
                except Exception as e:
                    logging.error(f"Batch processing failed: {e}")
        
        return merge_course_data(results)
    except Exception as e:
        logging.error(f"Error in parallel processing: {e}")
        raise

# DAG tasks for faculty info
def parallel_faculty_info(**context):
    try:
        cookie_output = context['task_instance'].xcom_pull(task_ids='get_cookies_task')
        course_list = context['task_instance'].xcom_pull(task_ids='get_course_list_task')
        
        if not course_list:
            raise ValueError("Course list is empty. Aborting.")
        
        logging.info(f"Length of course list: {len(course_list)}")
        logging.info(f"cookie_output: {cookie_output}")
        
        # Ensure proper JSON formatting
        if isinstance(course_list, str):
            course_list = json.loads(course_list)
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        
        results = parallel_process_with_threads(
            process_faculty_info_batch,
            json.dumps(cookie_output),
            course_list
        )
        logging.info(f"Length of results: {len(results)}")
        if not results:
            raise ValueError("Error in parallel_faculty_info")
        return json.dumps(results)
    except Exception as e:
        logging.error(f"Error in parallel_faculty_info: {str(e)}")
        raise ValueError(f"Error in parallel_faculty_info: {str(e)}") from e

# DAG tasks for course description
def parallel_course_description(**context):
    try:
        cookie_output = context['task_instance'].xcom_pull(task_ids='get_cookies_task')
        course_list = context['task_instance'].xcom_pull(task_ids='get_faculty_info_task')
        
        if not course_list:
            raise ValueError("Course list is empty. Aborting.")
        
        if isinstance(course_list, str):
            course_list = json.loads(course_list)
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        
        results = parallel_process_with_threads(
            process_description_batch,
            json.dumps(cookie_output),
            course_list
        )
        logging.info(f"Length of results: {len(results)}")
        if not results:
            raise ValueError("Error in parallel_course_description")
        return json.dumps(results)
    except Exception as e:
        logging.error(f"Error in parallel_course_description: {e}")
        raise
    
# DAG tasks for prerequisites
def parallel_prerequisites(**context):
    try:
        cookie_output = context['task_instance'].xcom_pull(task_ids='get_cookies_task')
        course_list = context['task_instance'].xcom_pull(task_ids='get_course_description_task')
        
        if not course_list:
            raise ValueError("Course list is empty. Aborting.")
        
        if isinstance(course_list, str):
            course_list = json.loads(course_list)
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        
        results = parallel_process_with_threads(
            process_prerequisites_batch,
            json.dumps(cookie_output),
            course_list
        )
        logging.info(f"Length of results: {len(results)}")
        if not results:
            raise ValueError("Error in parallel_prerequisites")
        return json.dumps(results)
    except Exception as e:
        logging.error(f"Error in parallel_prerequisites: {e}")
        raise