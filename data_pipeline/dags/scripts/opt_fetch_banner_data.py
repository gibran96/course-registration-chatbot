import ast
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from scripts.fetch_banner_data import get_course_description, get_course_prerequisites, get_faculty_info



def validate_input(func):
    def wrapper(*args, **kwargs):
        for arg in args:
            if not isinstance(arg, dict):
                raise ValueError(f"Expected dict, got {type(arg)}")
        for key, value in kwargs.items():
            if not isinstance(value, dict):
                raise ValueError(f"Expected dict for {key}, got {type(value)}")
        return func(*args, **kwargs)
    return wrapper

def get_xcom_data(ti, task_id):
    data = ti.xcom_pull(task_ids=task_id)
    if not data:
        raise ValueError(f"No data retrieved from task {task_id}")
    return parse_json_safely(data)

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
@validate_input
def process_faculty_info_batch(cookie_output, course_batch):
    """Process faculty info for a batch of courses."""
    cookie_output = parse_json_safely(cookie_output)
    course_batch = parse_json_safely(course_batch)
    return get_faculty_info(json.dumps(cookie_output), json.dumps(course_batch))

# Process course descriptions in parallel
@validate_input
def process_description_batch(cookie_output, course_batch):
    """Process course descriptions for a batch of courses"""
    cookie_output = parse_json_safely(cookie_output)
    course_batch = parse_json_safely(course_batch)
    return get_course_description(json.dumps(cookie_output), json.dumps(course_batch))

# Process prerequisites in parallel
@validate_input
def process_prerequisites_batch(cookie_output, course_batch):
    """Process prerequisites for a batch of courses"""
    cookie_output = parse_json_safely(cookie_output)
    course_batch = parse_json_safely(course_batch)
    return get_course_prerequisites(json.dumps(cookie_output), json.dumps(course_batch))

# Generic function for parallel processing
def parallel_process_with_threads(process_func, cookie_output, course_list, max_workers=5):
    """
    Generic function to process batches using ThreadPoolExecutor
    """
    try:
        # Ensure course_list is a dictionary
        course_list = parse_json_safely(course_list)
            
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
                    batch = future_to_batch[future]
                    logging.error(f"Batch processing failed {batch}: {str(e)}")
        
        return merge_course_data(results)
    except Exception as e:
        logging.error(f"Error in parallel processing: {e}")
        raise

# DAG tasks for faculty info
def parallel_faculty_info(**context):
    try:
        ti = context['ti']
        cookie_output = get_xcom_data(ti, 'get_cookies_task')
        course_list = get_xcom_data(ti, 'get_course_list_task')
        
        if not course_list:
            raise ValueError("Course list is empty. Aborting.")
        
        logging.info(f"Length of course list: {len(course_list)}")
        
        # Ensure proper JSON formatting
        if isinstance(course_list, str):
            course_list = json.loads(course_list)
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        
        results = parallel_process_with_threads(
            process_faculty_info_batch,
            cookie_output,
            course_list
        )
        logging.info(f"Length of results: {len(results)}")
        
        if not results:
            raise ValueError("Error in parallel_faculty_info")
        
        return results
    
    except Exception as e:
        logging.error(f"Error in parallel_faculty_info: {str(e)}")
        raise ValueError(f"Error in parallel_faculty_info: {str(e)}") from e

# DAG tasks for course description
def parallel_course_description(**context):
    try:
        ti = context['ti']
        cookie_output = get_xcom_data(ti, 'get_cookies_task')
        course_list = get_xcom_data(ti, 'get_faculty_info_task')
        
        if not course_list:
            raise ValueError("Course list is empty. Aborting.")
        
        logging.info(f"Length of course list: {len(course_list)}")
        
        if isinstance(course_list, str):
            course_list = ast.literal_eval(course_list)
        if isinstance(cookie_output, str):
            cookie_output = ast.literal_eval(cookie_output)
        
        results = parallel_process_with_threads(
            process_description_batch,
            cookie_output,
            course_list
        )
        logging.info(f"Length of results: {len(results)}")
        
        if not results:
            raise ValueError("Error in parallel_course_description")
        
        return results
    except Exception as e:
        logging.error(f"Error in parallel_course_description: {e}")
        raise
    
# DAG tasks for prerequisites
def parallel_prerequisites(**context):
    try:
        ti = context['ti']
        cookie_output = get_xcom_data(ti, 'get_cookies_task')
        course_list = get_xcom_data(ti, 'get_course_description_task')
        
        if not course_list:
            raise ValueError("Course list is empty. Aborting.")
        
        logging.info(f"Length of course list: {len(course_list)}")
        
        if isinstance(course_list, str):
            course_list = json.loads(course_list)
        if isinstance(cookie_output, str):
            cookie_output = json.loads(cookie_output)
        
        results = parallel_process_with_threads(
            process_prerequisites_batch,
            cookie_output,
            course_list
        )
        logging.info(f"Length of results: {len(results)}")
        
        if not results:
            raise ValueError("Error in parallel_prerequisites")
        
        return results
    except Exception as e:
        logging.error(f"Error in parallel_prerequisites: {e}")
        raise

def parse_json_safely(data):
    if isinstance(data, str):
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            logging.error(f"Failed to parse JSON: {data[:100]}...")  # Log first 100 chars
            return {}
    return data
