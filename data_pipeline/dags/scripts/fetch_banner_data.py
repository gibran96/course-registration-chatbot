
import json
import logging
from bs4 import BeautifulSoup
import pandas as pd
import requests
import csv
import os
from airflow.models import Variable
import ast

from scripts.extract_trace_data import clean_response

semester_map = {
    "10": "Fall",
    "30": "Spring",
    "40": "Summer 1",
    "50": "Summer Full",
    "60": "Summer 2"
}

# Function to fetch the cookies from the Banner API
def get_cookies(**context):
    # Fetching the base URL from the context
    base_url = context['dag_run'].conf.get('base_url', Variable.get('banner_base_url'))

    url = base_url + "term/search/"
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UT"
    }

    body = {
        "term": 202530,
        "studyPath" : "",
        "studyPathText" : "",
        "startDatepicker" : "",
        "endDatepicker" : ""
    }
    
    logging.info(f"Payload: {body}")
    
    try:
        response = requests.post(url, headers=headers, params=body)
        logging.info(f"Request made successfully")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch cookies: {e}")
        return None
    
    logging.info(f"Response: {response.status_code}")
    
    # if json contains key "regAllowed" then log error
    if "regAllowed" in response.json():
        logging.error("Error in fetching cookies")
        return None

    # Get the cookie from the response
    cookie = response.headers["Set-Cookie"]
    cookie = cookie.split(";")[0]
    
    logging.info(f"Response Headers: {response.headers}")

    # Get the JSESSIONID from the response
    jsessionid_ = response.headers["Set-Cookie"]
    jsessionid = jsessionid_.split(";")[0]

    # Get the nubanner-cookie from the response
    nubanneXr_cookie = response.headers["Set-Cookie"].split(";")[3].split(", ")[1]
    
    cookie_output = {
        "cookie": cookie,
        "jsessionid": jsessionid,
        "nubanner_cookie": nubanneXr_cookie,
        "base_url": base_url
    }

    context['ti'].xcom_push(key='cookie_output', value=cookie_output)

# Function to fetch the list of courses from the Banner API
def get_courses_list(**context):
    cookie_output = context['ti'].xcom_pull(task_ids='get_cookies_task', key='cookie_output')
    cookie, jsessionid, nubanner_cookie = cookie_output["cookie"], cookie_output["jsessionid"], cookie_output["nubanner_cookie"]
    base_url = cookie_output["base_url"]
    url = base_url + "searchResults/searchResults"

    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }
    
    term = 202530 # hardcoded for now TODO: Make this dynamic
    
    term_desc = get_semester_name(str(term))
    
    params = {
        "txt_subject": "CS",
        "txt_courseNumber": "",
        "txt_term": term,
        "startDatepicker": "",
        "endDatepicker": "",
        "pageOffset": 0,
        "pageMaxSize": 501, # this is the maximum number of courses that can be fetched in one request
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
    
    course_list_df = pd.DataFrame(course_data)
    logging.info(f"Number of courses fetched: {len(course_list_df)}")
    
    # Push DataFrame to XCom
    context['ti'].xcom_push(key='course_list_df', value=course_list_df)

# Function to fetch the faculty info from the Banner API
def get_faculty_info(cookie_output, course_list_df):
    cookie, jsessionid, nubanner_cookie = cookie_output["cookie"], cookie_output["jsessionid"], cookie_output["nubanner_cookie"]
    base_url = cookie_output["base_url"]
    url = base_url + "/searchResults/getFacultyMeetingTimes"
    
    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }
    
    term = 202530 # hardcoded for now

    params = {
        "term": term,
        "courseReferenceNumber": ""
    }
    
    # loop through the course_list_df and fetch the faculty info for each crn
    for index, row in course_list_df.iterrows():
        course_ref_num = row["crn"]
        params["courseReferenceNumber"] = course_ref_num       
        
        try:  
            response = requests.post(url, headers=headers, params=params)
            response.raise_for_status()  # Raises an exception for bad status codes
            
            data = response.json()["fmt"][0]
            
            course_list_df.loc[index, "faculty_name"] = data["faculty"][0]["displayName"] if data["faculty"] else ""
            course_list_df.loc[index, "begin_time"] = data["meetingTime"]["beginTime"] if data["meetingTime"]["beginTime"] else ""
            course_list_df.loc[index, "end_time"] = data["meetingTime"]["endTime"] if data["meetingTime"]["endTime"] else ""
            course_list_df.loc[index, "days"] = get_days(data["meetingTime"]) if data["meetingTime"] else ""
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch faculty info for crn: {course_ref_num}. Error: {e}")
            # Set empty values for failed requests
            course_list_df.loc[index, ["faculty_name", "begin_time", "end_time", "days"]] = ""
        except (KeyError, IndexError) as e:
            logging.error(f"Error parsing data for crn: {course_ref_num}. Error: {e}")
            course_list_df.loc[index, ["faculty_name", "begin_time", "end_time", "days"]] = ""
            
    return course_list_df


# Function to fetch the course description from the Banner API
def get_course_description(cookie_output, course_list_df):    
    cookie, jsessionid, nubanner_cookie = cookie_output["cookie"], cookie_output["jsessionid"], cookie_output["nubanner_cookie"]
    base_url = cookie_output["base_url"]
    url = base_url + "/searchResults/getCourseDescription"
    
    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }
    
    term = 202530 # hardcoded for now

    params = {
        "term": term,
        "courseReferenceNumber": ""
    }
    
    for index, row in course_list_df.iterrows():
        course_ref_num = row["crn"]
        params["courseReferenceNumber"] = course_ref_num       
        
        try:
            response = requests.post(url, headers=headers, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch description for CRN: {course_ref_num} - {e}")
            continue

        # Parse HTML response
        soup = BeautifulSoup(response.text, 'html.parser')
        description_section = soup.find("section", {"aria-labelledby": "courseDescription"})
        
        if description_section:
            # Extract and clean up the text
            description_text = description_section.get_text(strip=True)
            description_text = clean_response(description_text)
            course_list_df.loc[index, "course_description"] = description_text
        else:
            course_list_df.loc[index, "course_description"] = "No description available."
            logging.warning(f"No description found for CRN: {course_ref_num}")

    return course_list_df

# Function to fetch the prerequisites for the courses from the Banner API
def get_course_prerequisites(cookie_output, course_list_df):
    cookie, jsessionid, nubanner_cookie = cookie_output["cookie"], cookie_output["jsessionid"], cookie_output["nubanner_cookie"]
    base_url = cookie_output["base_url"]
    url = base_url + "/searchResults/getSectionPrerequisites"
    
    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }
    
    term = 202530 # hardcoded for now

    params = {
        "term": term,
        "courseReferenceNumber": ""
    }
    
    for index, row in course_list_df.iterrows():
        course_ref_num = row["crn"]
        params["courseReferenceNumber"] = course_ref_num       
        
        try:
            response = requests.post(url, headers=headers, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch prerequisites for CRN: {course_ref_num} - {e}")
            continue

        # Parse HTML response
        soup = BeautifulSoup(response.text, 'html.parser')
        prerequisites = []
        
        # Locate the prerequisite table
        table = soup.find("table", class_="basePreqTable")
        
        if table:
            # Parse each row in the table body
            for row in table.find("tbody").find_all("tr"):
                cells = row.find_all("td")
                prereq = {
                    "and_or": cells[0].text.strip(),
                    "subject": cells[4].text.strip(),
                    "course_number": cells[5].text.strip(),
                    "level": cells[6].text.strip(),
                    "grade": cells[7].text.strip()
                }
                prerequisites.append(prereq)
        else:
            logging.warning(f"No prerequisites found for CRN: {course_ref_num}")
        
        course_list_df.loc[index, "prereq"] = prerequisites
    
    return course_list_df

# Function to dump the course data to a CSV file
def dump_to_csv(**context):
    course_list_df = context['ti'].xcom_pull(task_ids='get_prerequisites_task', key='course_list_df')
    
    if course_list_df.empty:
        raise ValueError("Course_data is None or empty, unable to dump to CSV.")
    
    course_list_df_filled = course_list_df.fillna("")
    
    course_list_df_filled["prereq"] = course_list_df_filled["prereq"].apply(lambda x: json.dumps(x) if x else "")
    
    # print the length of the course_data
    logging.info(f"Length of course_data: {course_list_df.shape[0]}")

    output_path = context['dag_run'].conf.get('output_path', '/tmp/banner_data')
    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, "banner_course_data.csv")
    
    course_list_df_filled.to_csv(file_path, index=False, na_rep="", quoting=1)    
                
                
# Function to get the semester name from the term
def get_semester_name(term):
    semester = term[-2:]
    return semester_map[semester] + " " + term[:4]

# Function to get the days of the course lecture
def get_days(meeting_time):
    if isinstance(meeting_time, tuple):
        meeting_time = meeting_time[0]
    days = [day for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'] 
            if meeting_time.get(day, False)]
    return ";".join(days)

# TODO - Implement this function
def get_next_term(cookie_output):
    pass
