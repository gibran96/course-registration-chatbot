# New utility functions for batch processing
import ast
import json

from scripts.fetch_banner_data import get_course_description, get_course_prerequisites, get_faculty_info


def split_course_list(course_list, batch_size=10):
    """Split course list into smaller batches"""
    course_items = list(course_list.items())
    for i in range(0, len(course_items), batch_size):
        batch = dict(course_items[i:i + batch_size])
        yield batch

def merge_course_data(batch_results):
    """Merge results from parallel processing back into a single dictionary"""
    merged_data = {}
    for batch in batch_results:
        if batch:  # Check if batch is not None
            merged_data.update(ast.literal_eval(batch))
    return merged_data

def process_faculty_info_batch(cookie_output, course_batch):
    """Process faculty info for a batch of courses"""
    return get_faculty_info(cookie_output, json.dumps(course_batch))

def process_description_batch(cookie_output, course_batch):
    """Process course descriptions for a batch of courses"""
    return get_course_description(cookie_output, json.dumps(course_batch))

def process_prerequisites_batch(cookie_output, course_batch):
    """Process prerequisites for a batch of courses"""
    return get_course_prerequisites(cookie_output, json.dumps(course_batch))

# Modified main processing functions
def parallel_faculty_info(**context):
    cookie_output = context['task_instance'].xcom_pull(task_ids='get_cookies_task')
    course_list = ast.literal_eval(context['task_instance'].xcom_pull(task_ids='get_course_list_task'))
    
    batches = list(split_course_list(course_list))
    batch_results = []
    
    # Process each batch and collect results
    for batch in batches:
        result = process_faculty_info_batch(cookie_output, batch)
        if result:
            batch_results.append(result)
    
    # Merge results
    return json.dumps(merge_course_data(batch_results))

def parallel_course_description(**context):
    cookie_output = context['task_instance'].xcom_pull(task_ids='get_cookies_task')
    course_list = ast.literal_eval(context['task_instance'].xcom_pull(task_ids='get_faculty_info_parallel'))
    
    batches = list(split_course_list(course_list))
    batch_results = []
    
    for batch in batches:
        result = process_description_batch(cookie_output, batch)
        if result:
            batch_results.append(result)
    
    return json.dumps(merge_course_data(batch_results))

def parallel_prerequisites(**context):
    cookie_output = context['task_instance'].xcom_pull(task_ids='get_cookies_task')
    course_list = ast.literal_eval(context['task_instance'].xcom_pull(task_ids='get_course_description_parallel'))
    
    batches = list(split_course_list(course_list))
    batch_results = []
    
    for batch in batches:
        result = process_prerequisites_batch(cookie_output, batch)
        if result:
            batch_results.append(result)
    
    return json.dumps(merge_course_data(batch_results))