import logging
import random
from scripts.seed_data import topics, seed_query_list


def get_initial_queries(**context):
    """
    This function generates initial queries for the LLM by selecting a random
    element from the list of topics, course names, and professor names. The
    selected element is then used to format the queries in the seed data list
    and the resulting list of queries is stored in XCom for the DAG to use.

    :param context: The context object containing task instance and other metadata
    :return: The list of initial queries or "stop_task" if the target sample count has been reached
    """
    task_status = context['ti'].xcom_pull(task_ids='check_sample_count', key='task_status')
    logging.info(f"Task status: {task_status}")

    if task_status == "stop_task":
        return "stop_task"
    
    else:

        course_list = context['ti'].xcom_pull(task_ids='get_bq_data', key='course_list')
        prof_list = context['ti'].xcom_pull(task_ids='get_bq_data', key='prof_list')

        col_names = ['topic', 'course_name', 'professor_name']

        all_queries = []
        for selected_col in col_names:
            if selected_col == 'topic':
                topic = random.choice(topics)
                # topic = 'Software Development'
                query_subset = [query for query in seed_query_list if selected_col in query]
                queries = [query.format(topic=topic) for query in query_subset]
            elif selected_col == 'course_name':
                course_name = random.choice(course_list)
                # course_name = 'Advanced Software Development'
                query_subset = [query for query in seed_query_list if selected_col in query]
                queries = [query.format(course_name=course_name) for query in query_subset]
            elif selected_col == 'professor_name':
                professor_name = random.choice(prof_list)
                # professor_name = 'Skoteiniotis, Therapon'
                query_subset = [query for query in seed_query_list if selected_col in query]
                queries = [query.format(professor_name=professor_name) for query in query_subset]

            all_queries.extend(queries)

        context['ti'].xcom_push(key='initial_queries', value=all_queries)
        logging.info(f'Initial queries: {len(all_queries)}' )
        logging.info(f'Initial queries: {all_queries}' )
        return "generate_samples"