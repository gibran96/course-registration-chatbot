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

def send_missing_faculty_mail(course_list_str):
    """
    Send an email to the specified email address with a list of courses that have no faculty name specified.
    Args:
        course_list_str (str): A string containing a list of courses with no faculty name specified.
    """
    email_subject = "Notification: Missing Faculty Names in Banner Course List"
    email_body = f"""The following courses have no faculty name specified:\n\n{course_list_str}.\n\n 
                     They have been removed from the course list."""

    # Set up the Airflow EmailOperator to send the email
    email_operator = EmailOperator(
        task_id="notify_missing_faculty",
        to='mlopsggmu@gmail.com',
        subject=email_subject,
        html_content=email_body
    )
    
    email_operator.execute(context=None)