from airflow.operators.email import EmailOperator

def send_anomaly_email(anomaly_text):
    """
    Send an email with the anomaly text to the specified email address.
    Args:
        anomaly_text (str): The text to include in the email.
    """
    # Send an email with the anomaly text
    
    email_task = EmailOperator(
        task_id='anomaly_email',
        to='mlopsggmu@gmail.com',
        subject='Anomalies have been detected in your data',
        html_content=f"""<p>Dear User,</p>
                        <p>Anomalies have been detected in your data:</p>
                        <p>{anomaly_text}</p>
                        <p>Kind regards,<br/>Your Data Pipeline</p>""",

    )
    email_task.execute(context=None)