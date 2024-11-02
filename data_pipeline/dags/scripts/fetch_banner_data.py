
import json
import logging
from bs4 import BeautifulSoup
import requests
import csv
import os
import pandas as pd
import numpy as np
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

    return cookie_output

# TODO - Implement this function
def get_next_term(cookie_output):
    pass

# Function to fetch the list of courses from the Banner API
def get_courses_list(cookie_output):
    cookie_output = ast.literal_eval(cookie_output)
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
        logging.info("Request made successfully")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch course list: {e}")
        return []
    
    total_count = response.json()["totalCount"]
    logging.info(f"Response: {response.status_code}")
    logging.info(f"Number of courses: {total_count}")
    
    # Get the JSON response
    response_json = response.json()
    
    course_data = {}

    for course in response_json["data"]:
        course_data[course["courseReferenceNumber"]] = {
            "crn": course["courseReferenceNumber"],
            "campus_description": course["campusDescription"],
            "course_title": course["courseTitle"],
            "subject_course": course["subjectCourse"],
            "term": term_desc,
        }

    return course_data

# Function to fetch the faculty info from the Banner API
def get_faculty_info(cookie_output, course_list):
    cookie_output = ast.literal_eval(cookie_output)
    course_list = ast.literal_eval(course_list)
    
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
    
    for course in course_list:
        course_ref_num = course_list[course]["crn"]
        params["courseReferenceNumber"] = course_ref_num       
        
        try:  
            response = requests.post(url, headers=headers, params=params)
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch faculty info for crn: {course_ref_num}. Error: {e}")
        
        data = response.json()["fmt"][0]
        
        if response.status_code == 200:
            course_list[course]["faculty_name"] = data["faculty"][0]["displayName"] if len(data["faculty"]) != 0 else ""
            course_list[course]["begin_time"] = data["meetingTime"]["beginTime"] if data["meetingTime"] else ""
            course_list[course]["end_time"] = data["meetingTime"]["endTime"] if data["meetingTime"] else ""
            course_list[course]["days"] = get_days(data["meetingTime"]) if data["meetingTime"] else ""
        else:
            # Handle cases where the request was unsuccessful
            course_list[course]["faculty_name"] = ""
            course_list[course]["begin_time"] = ""
            course_list[course]["end_time"] = ""
            course_list[course]["days"] = ""
            logging.error(f"Failed to fetch faculty and meeting info for course: {course_ref_num}")
    
    return course_list

# Function to fetch the course description from the Banner API
def get_course_description(cookie_output, course_list):
    cookie_output = ast.literal_eval(cookie_output)
    course_list = ast.literal_eval(course_list)
    
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
    
    for course in course_list:
        course_ref_num = course_list[course]["crn"]
        params["courseReferenceNumber"] = course_ref_num       
        
        try:  
            response = requests.post(url, headers=headers, params=params)
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch course description: {e}")
            continue
        
        if response.status_code == 200:
            # Parse the HTML response to extract course description
            soup = BeautifulSoup(response.text, "html.parser")
            description_section = soup.find("section", {"aria-labelledby": "courseDescription"})
            
            if description_section:
                # Extract and clean up the text
                description_text = description_section.get_text(strip=True)
                description_text = clean_response(description_text)
                course_list[course]["course_description"] = description_text
            else:
                course_list[course]["course_description"] = "No description available."
                logging.warning(f"No description found for course: {course_ref_num}")
        else:
            # Handle cases where the request was unsuccessful
            course_list[course]["course_description"] = "Failed to fetch description."
            logging.error(f"Failed to fetch description for course: {course_ref_num}")

    return course_list

# Function to fetch the prerequisites for the courses from the Banner API
def get_course_prerequisites(cookie_output, course_list):
    cookie_output = ast.literal_eval(cookie_output)
    course_list = ast.literal_eval(course_list)
    
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
    
    for course in course_list:
        course_ref_num = course_list[course]["crn"]
        params["courseReferenceNumber"] = course_ref_num       
        
        try:
            response = requests.get(url, headers=headers, params=params)
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
        
        course_list[course]["prereq"] = prerequisites
    
    return course_list

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
    

# Function to dump the course data to a CSV file
def dump_to_csv(course_data, **context):
    course_data = ast.literal_eval(course_data)
    
    # print the length of the course_data
    logging.info(f"Length of course_data: {len(course_data)}")

    output_path = context['dag_run'].conf.get('output_path', '/tmp/banner_data')

    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, "banner_course_data.csv")
    
    with open(file_path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["crn", "course_title", "subject_course", "faculty_name", "campus_description", 
                             "course_description", "term", "begin_time", "end_time", "days", "prereq"])
            for course in course_data:
                writer.writerow([course_data[course]["crn"], course_data[course]["course_title"], 
                                 course_data[course]["subject_course"], course_data[course]["faculty_name"], 
                                 course_data[course]["campus_description"], course_data[course]["course_description"], 
                                 course_data[course]["term"], course_data[course]["begin_time"],
                                 course_data[course]["end_time"], course_data[course]["days"], course_data[course].get("prereq", [])])    
