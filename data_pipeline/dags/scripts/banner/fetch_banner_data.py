import logging
from bs4 import BeautifulSoup
import pandas as pd
import requests
import os
from airflow.models import Variable

from scripts.data.data_anomalies import check_missing_faculty
from scripts.data.data_utils import clean_response, get_days, get_semester_name
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

def get_cookies(**context):
    """
    Fetches the cookies from the Banner API, given a base URL and a term (currently hardcoded to 202530).

    This function makes a POST request to the Banner API with the specified term and empty study path,
    and then extracts the cookie, JSESSIONID, and nubanner-cookie from the response. The cookie is
    stored in the XCom value `cookie_output` and can be accessed by other tasks in the DAG.

    :param context: The Airflow context
    :return: A dictionary containing the cookie, JSESSIONID, and nubanner-cookie, or None if the request fails.
    """
    # Get the base URL from the dag_run.conf or the Variable banner_base_url
    base_url = context['dag_run'].conf.get('base_url', Variable.get('banner_base_url'))
    url = base_url + "/term/search/"

    # Set the headers for the POST request
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UT"
    }

    # Set the body for the POST request
    body = {
        "term": 202530,
        "studyPath" : "",
        "studyPathText" : "",
        "startDatepicker" : "",
        "endDatepicker" : ""
    }
    logging.info(f"Payload: {body}")

    # Make the POST request
    try:
        response = requests.post(url, headers=headers, params=body)
        logging.info(f"Request made successfully")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch cookies: {e}")
        return None

    logging.info(f"Response: {response.status_code}")
    # If the response contains the key "regAllowed", log an error
    if "regAllowed" in response.json():
        logging.error("Error in fetching cookies")
        return None

    # Get the cookie, JSESSIONID, and nubanner-cookie from the response
    cookie = response.headers["Set-Cookie"]
    cookie = cookie.split(";")[0]
    jsessionid_ = response.headers["Set-Cookie"]
    jsessionid = jsessionid_.split(";")[0]
    nubanneXr_cookie = response.headers["Set-Cookie"].split(";")[3].split(", ")[1]

    # Store the cookie, JSESSIONID, and nubanner-cookie in the XCom value cookie_output
    cookie_output = {
        "cookie": cookie,
        "jsessionid": jsessionid,
        "nubanner_cookie": nubanneXr_cookie,
        "base_url": base_url
    }
    context['ti'].xcom_push(key='cookie_output', value=cookie_output)


def get_courses_list(**context):
    """
    Fetches the list of courses from the Banner API using the provided cookie_output.
    The task pushes the DataFrame of course data to XCom under the key course_list_df.
    
    :param context: The Airflow context
    """
    cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
    cookie, jsessionid, nubanner_cookie = cookie_output["cookie"], cookie_output["jsessionid"], cookie_output["nubanner_cookie"]
    base_url = cookie_output["base_url"]
    url = base_url + "searchResults/searchResults"

    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }
    
    # hardcode the term for now TODO: Make this dynamic
    term = 202530
    term_desc = get_semester_name(str(term))
    
    params = {
        "txt_subject": "CS",
        "txt_courseNumber": "",
        "txt_term": term,
        "startDatepicker": "",
        "endDatepicker": "",
        # Fetch the maximum number of courses in one request
        "pageOffset": 0,
        "pageMaxSize": 501,
        "sortColumn": "subjectDescription",
        "sortDirection": "asc"
    }
    logging.info(f"Params: {params}")
    logging.info(f"Headers: {headers}")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Ensure we catch any HTTP errors
        logging.info("Request made successfully")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch course list: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if the request fails
    
    response_json = response.json()
    
    if "totalCount" in response_json:
        total_count = response_json["totalCount"]
        logging.info(f"Response: {response.status_code}")
        logging.info(f"Number of courses: {total_count}")
    
    # Prepare data for DataFrame
    course_data = [
        {
            "crn": course["courseReferenceNumber"],
            "campus_description": course["campusDescription"],
            "course_title": course["courseTitle"],
            "subject_course": course["subjectCourse"],
            "faculty_name": "",  # Placeholder for now
            "course_description": "",
            "term": term_desc,
            "begin_time": "",
            "end_time": "",
            "days": "",
            "prereq": ""
        }
        for course in response_json.get("data", [])
    ]
    
    # testing the action again to see if it works
    
    course_list_df = pd.DataFrame(course_data)
    logging.info(f"Number of courses fetched: {len(course_list_df)}")
    
    # Push DataFrame to XCom
    context['ti'].xcom_push(key='course_list_df', value=course_list_df)


def get_faculty_info(cookie_output, course_list_df):
    """
    Fetch the faculty information for given course reference numbers.

    :param cookie_output: The cookie output from the get_cookies function with the necessary cookies.
    :param course_list_df: DataFrame containing course list details.
    :param term: The term value to be used in API calls.
    :return: Updated DataFrame with faculty info.
    """
    base_url = cookie_output["base_url"]
    headers = {"Cookie": f"{cookie_output['jsessionid']}; {cookie_output['nubanner_cookie']}"}
    url = base_url + "searchResults/getFacultyMeetingTimes"
    term = 202530
    
    for index, row in course_list_df.iterrows():
        crn = row["crn"]
        params = {"term": term, "courseReferenceNumber": crn}
        
        try:
            response = make_api_call_with_retry(url, headers, params)
            data = response.json().get("fmt", [{}])[0]
            faculty = data.get("faculty", [])
            meeting_time = data.get("meetingTime", {})
            
            course_list_df.at[index, "faculty_name"] = faculty[0]["displayName"] if faculty else ""
            course_list_df.at[index, "begin_time"] = meeting_time.get("beginTime", "")
            course_list_df.at[index, "end_time"] = meeting_time.get("endTime", "")
            course_list_df.at[index, "days"] = get_days(meeting_time)
        except (Exception) as e:
            logging.error(f"Failed to fetch faculty info for CRN {crn}: {e}")
            course_list_df.at[index, ["faculty_name", "begin_time", "end_time", "days"]] = ""

    return course_list_df


def get_course_description(cookie_output, course_list_df):
    """
    Fetches course descriptions for given CRNs.

    :param cookie_output: The cookie output from the get_cookies function with the necessary cookies.
    :param course_list_df: DataFrame containing course list details.
    :param term: The term value to be used in API calls.
    :return: Updated DataFrame with course descriptions.
    """
    base_url = cookie_output["base_url"]
    headers = {"Cookie": f"{cookie_output['jsessionid']}; {cookie_output['nubanner_cookie']}"}
    url = base_url + "searchResults/getCourseDescription"
    term = 202530

    for index, row in course_list_df.iterrows():
        crn = row["crn"]
        params = {"term": term, "courseReferenceNumber": crn}

        try:
            response = make_api_call_with_retry(url, headers, params)
            soup = BeautifulSoup(response.text, "html.parser")
            description_section = soup.find("section", {"aria-labelledby": "courseDescription"})
            
            if description_section:
                description_text = description_section.get_text(strip=True)
                course_list_df.at[index, "course_description"] = clean_response(description_text)
            else:
                course_list_df.at[index, "course_description"] = "No description available."
                logging.warning(f"No description found for CRN: {crn}")
        except (Exception) as e:
            logging.error(f"Failed to fetch course description for CRN {crn}: {e}")
            course_list_df.at[index, "course_description"] = ""

    return course_list_df

def get_course_prerequisites(cookie_output, course_list_df):
    """
    Fetches course prerequisites for given CRNs.

    :param cookie_output: The cookie output from the get_cookies function with the necessary cookies.
    :param course_list_df: DataFrame containing course list details.
    :param term: The term value to be used in API calls.
    :return: Updated DataFrame with course prerequisites.
    """
    base_url = cookie_output["base_url"]
    headers = {"Cookie": f"{cookie_output['jsessionid']}; {cookie_output['nubanner_cookie']}"}
    url = base_url + "searchResults/getSectionPrerequisites"
    
    term = 202530

    for index, row in course_list_df.iterrows():
        crn = row["crn"]
        params = {"term": term, "courseReferenceNumber": crn}

        try:
            response = make_api_call_with_retry(url, headers, params)
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", class_="basePreqTable")
            prerequisites = []

            if table:
                for row in table.find("tbody").find_all("tr"):
                    cells = row.find_all("td")
                    prereq = {
                        "and_or": cells[0].text.strip(),
                        "subject": cells[4].text.strip(),
                        "course_number": cells[5].text.strip(),
                        "level": cells[6].text.strip(),
                        "grade": cells[7].text.strip(),
                    }
                    prerequisites.append(prereq)
            else:
                logging.warning(f"No prerequisites found for CRN: {crn}")
            
            course_list_df.at[index, "prereq"] = prerequisites
        except (Exception) as e:
            logging.error(f"Failed to fetch prerequisites for CRN {crn}: {e}")
            course_list_df.at[index, "prereq"] = []

    return course_list_df


def dump_to_csv(**context):
    """
    Dumps the course list DataFrame to a CSV file.

    This function takes the course list DataFrame from the XCom context and dumps it to a CSV file. The
    path to the file is determined by the 'output_path' key in the DAG run configuration. If the key is
    not present, it defaults to /tmp/banner_data.

    The function also replaces newline, tab, and carriage return characters with spaces in the 'prereq'
    column, and fills NaN values with empty strings.

    Raises:
        ValueError: If the course_data is None or empty.

    """
    course_list_df = context['ti'].xcom_pull(task_ids='get_prerequisites_task', key='course_list_df')
    
    if course_list_df.empty:
        raise ValueError("Course_data is None or empty, unable to dump to CSV.")
    
    # Check for missing faculty names
    course_list_df = check_missing_faculty(course_list_df)
    
    course_list_df_filled = course_list_df.fillna("")
    
    course_list_df_filled["prereq"] = course_list_df_filled["prereq"].apply(
        lambda x: str(x).replace("\n", " ").replace("\r", " ").replace("\t", " ")
    )
    
    # print the length of the course_data
    logging.info(f"Length of course_data: {course_list_df.shape[0]}")

    output_path = context['dag_run'].conf.get('output_path', '/tmp/banner_data')
    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, "banner_course_data.csv")
    
    course_list_df_filled.to_csv(file_path, index=False, na_rep="", quoting=1)    
    
# Retry decorator for API calls
@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff
    retry=retry_if_exception_type(requests.exceptions.RequestException),  # Retry on request exceptions
)
def make_api_call_with_retry(url, headers, params):
    """
    Make an API call with retries on failure.

    :param url: The API endpoint.
    :param headers: Headers for the API request.
    :param params: Parameters for the API request.
    :return: The response object if successful.
    """
    logging.info(f"Making API call: {url} with params: {params} and headers: {headers}")
    response = requests.post(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response

