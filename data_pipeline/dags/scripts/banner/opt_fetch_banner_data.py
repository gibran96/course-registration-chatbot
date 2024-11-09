import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import pandas as pd

from scripts.banner.fetch_banner_data import get_course_description, get_course_prerequisites, get_faculty_info


def split_course_list_df(course_list_df, batch_size=10):
    """
    Split a DataFrame of course data into smaller batches for parallel processing.

    :param course_list_df: DataFrame of course data
    :param batch_size: Number of rows to include in each batch (default=10)
    :yield: DataFrame subset of course_list_df with batch_size rows
    """
    for i in range(0, len(course_list_df), batch_size):
        yield course_list_df.iloc[i:i + batch_size]

def merge_course_data_df(batch_results):
    """
    Merge a list of DataFrame objects into a single DataFrame.

    Args:
        batch_results (list): A list of pandas DataFrame objects to be concatenated.

    Returns:
        pandas.DataFrame: A single DataFrame containing all rows from the input DataFrames,
        with the index reset.
    """
    return pd.concat(batch_results, ignore_index=True)

def process_faculty_info_batch_df(cookie_output, course_batch_df):
    """
    Process the faculty information for a batch of courses.

    Args:
        cookie_output (dict): The output of the get_cookies function
        course_batch_df (pandas.DataFrame): A DataFrame subset of the course_list_df
            containing the courses to process.

    Returns:
        pandas.DataFrame: The course_batch_df with additional columns for faculty info.
    """
    return get_faculty_info(cookie_output, course_batch_df)

def process_description_batch_df(cookie_output, course_batch_df):
    """
    Process course descriptions for a batch of courses.

    Args:
        cookie_output (dict): The output of the get_cookies function
        course_batch_df (pandas.DataFrame): A DataFrame subset of the course_list_df
            containing the courses to process.

    Returns:
        pandas.DataFrame: The course_batch_df with additional columns for course description.
    """
    return get_course_description(cookie_output, course_batch_df)

def process_prerequisites_batch_df(cookie_output, course_batch_df):
    """
    Process course prerequisites for a batch of courses.

    Args:
        cookie_output (dict): The output of the get_cookies function
        course_batch_df (pandas.DataFrame): A DataFrame subset of the course_list_df
            containing the courses to process.

    Returns:
        pandas.DataFrame: The course_batch_df with additional columns for course prerequisites.
    """
    return get_course_prerequisites(cookie_output, course_batch_df)

def parallel_process_with_threads_df(process_func, cookie_output, course_list_df, max_workers=5):
    """
    Process a course list DataFrame with a given function in parallel using multiple threads.

    Takes a function that processes a DataFrame subset of the course_list_df, and applies it to
    each subset in parallel using multiple threads. Returns the concatenated DataFrames.

    Args:
        process_func (callable): The function to apply to each DataFrame subset.
        cookie_output (dict): The output of the get_cookies function.
        course_list_df (pandas.DataFrame): The DataFrame to process.
        max_workers (int): The number of threads to use in parallel. Defaults to 5.

    Returns:
        pandas.DataFrame: The concatenated DataFrames.
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


def parallel_faculty_info(**context):
    """
    Pulls the course list DataFrame from the XCom context, processes it in parallel
    using the process_faculty_info_batch_df function, and pushes the result back into
    the XCom context.

    Args:
        **context: The Airflow context.

    Raises:
        ValueError: If the course_list_df is None or empty.
    """
    try:
        cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
        course_list_df = context['ti'].xcom_pull(task_ids='get_course_list_task', key='course_list_df')

        course_list_df = course_list_df.copy()
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


def parallel_course_description(**context):
    """
    Pulls the course list DataFrame from the XCom context, processes it in parallel
    using the process_description_batch_df function, and pushes the result back into
    the XCom context.

    Args:
        **context: The Airflow context.

    Raises:
        ValueError: If the course_list_df is None or empty.
    """
    try:
        cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
        course_list_df = context['ti'].xcom_pull(task_ids='get_faculty_info_task', key='course_list_df')
        
        course_list_df = course_list_df.copy()
        logging.info(f"Length of course list DataFrame: {len(course_list_df)}")
        
        results_df = parallel_process_with_threads_df(
            process_description_batch_df,
            cookie_output,
            course_list_df
        )
        logging.info(f"Length of results: {len(results_df)}")
        
        if results_df.empty:
            raise ValueError("Error in parallel_course_description")
        
        context['ti'].xcom_push(key='course_list_df', value=results_df)
    except Exception as e:
        logging.error(f"Error in parallel_course_description: {e}")
        raise
    

def parallel_prerequisites(**context):
    """
    Pulls the course list DataFrame from the XCom context, processes it in parallel
    using the process_prerequisites_batch_df function, and pushes the result back into
    the XCom context.

    Args:
        **context: The Airflow context.

    Raises:
        ValueError: If the course_list_df is None or empty.
    """
    
    try:
        cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
        course_list_df = context['ti'].xcom_pull(task_ids='get_course_description_task', key='course_list_df')
        
        course_list_df = course_list_df.copy()
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
