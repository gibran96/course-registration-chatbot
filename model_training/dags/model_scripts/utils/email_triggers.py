from airflow.operators.email import EmailOperator

def send_bias_detected_mail(query_report_df, prof_report_df, total_query_bucket_report_df):
    # get list of professors with bias and the corresponding queries
    """
    Sends an email report on bias detected in professors and associated queries.

    Args:
        query_report_df (pd.DataFrame): DataFrame containing query buckets and associated sentiment scores.
        prof_report_df (pd.DataFrame): DataFrame containing professors and their sentiment scores.
        total_query_bucket_report_df (pd.DataFrame): DataFrame containing overall sentiment scores by query bucket.

    This function identifies professors and queries that show bias by analyzing sentiment scores. It sends an email
    containing the list of professors and queries with detected bias, and highlights queries with high sentiment scores.
    """
    profs = [{prof, score} for prof, score in zip(prof_report_df['prof_name'], prof_report_df['mean'])]
    list_of_queries = [query for query in query_report_df['query_bucket'].unique() if query in total_query_bucket_report_df['query_bucket'].unique()]
    queries = [{query, score} for query, score in zip(query_report_df['query_bucket'], query_report_df['mean'])]
    
    # Send an email with the bias report
    email_operator = EmailOperator(
        task_id='send_bias_report_email',
        to='mlopsggmu@gmail.com',
        subject='Bias Detection Report',
        html_content=f"""<p>Dear User,</p>
                        <p>Bias has been detected in the model for the following professors:</p>
                        <ul>
                            {', '.join([f'<li>{prof}: {score}</li>' for prof, score in profs])}
                        </ul>
                        <p>For the following queries:</p>
                        <ul>
                            {', '.join([f'<li>{query}</li>' for query in list_of_queries])}
                        </ul>
                        <p>Queries with high sentiment scores:</p>
                        <ul>
                            {', '.join([f'<li>{query}: {score}</li>' for query, score in queries])}
                        </ul>
                        <p>Kind regards,<br/>Your Model Pipeline</p>""",
    )
    
    email_operator.execute(context=None)
    
    