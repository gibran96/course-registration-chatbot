
import logging
from bs4 import BeautifulSoup
import requests
import csv
import os
import pandas as pd
import numpy as np
from airflow.models import Variable

def get_cookies(**context):
    base_url = context['dag_run'].conf.get('base_url', Variable.get('banner_base_url'))
    # base_url = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    
    url = base_url + "term/search"

    headers = {
        "Content-Type": "application/json"
    }

    payload = {
        "term": "202510",
        "studyPath" : "",
        "studyPathText" : "",
        "startDatepicker" : "",
        "endDatepicker" : "",
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch cookies: {e}")
        return None

    print("Response: ", response.text)

    # Get the cookie from the response
    cookie = response.headers["Set-Cookie"]
    cookie = cookie.split(";")[0]

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


def get_courses_list(cookie_output):
    cookie, jsessionid, nubanner_cookie = cookie_output["cookie"], cookie_output["jsessionid"], cookie_output["nubanner_cookie"]
    
    # base_url = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    base_url = cookie_output["base_url"]
        
    url = base_url + "searchResults/searchResults"

    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }
    
    # TODO: Check the next semester open for registration and update the txt_term
    
    term = "202510" # hardcoded for now
    
    # Add the query parameters to the URL using requests library
    params = {
        "txt_subject": "CS",
        "txt_courseNumber": "",
        "txt_term": term,
        "startDatepicker": "",
        "endDatepicker": "",
        "pageOffset": 0,
        "pageMaxSize": 500,
        "sortColumn": "subjectDescription",
        "sortDirection": "asc"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch course list: {e}")
        return []

    # Get the JSON response
    response_json = response.json()

    print("Number of courses: ", response_json["totalCount"])
    
    course_data = {}

    for course in response_json["data"]:
        course_data[course["courseReferenceNumber"]] = {
            "courseReferenceNumber": course["courseReferenceNumber"],
            "campusDescription": course["campusDescription"],
            "courseTitle": course["courseTitle"],
            "subjectCourse": course["subjectCourse"],
            "facultyName": course["faculty"][0]["displayName"],
            "term": term
        }

    return course_data

def get_course_description(cookie_output, course_list):
    """ Get the description of the course """
    
    # Get the cookie, JSESSIONID and nubanner-cookie
    cookie, jsessionid, nubanner_cookie = cookie_output["cookie"], cookie_output["jsessionid"], cookie_output["nubanner_cookie"]
    
    # base_url = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    base_url = cookie_output["base_url"]

    # Send a GET request to Banner API @ /courseDescription to get the description of the course
    url = base_url + "/searchResults/getCourseDescription"
    
    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }

    params = {
        "term": "202510",
        "courseReferenceNumber": ""
    }
    
    for course in course_list:
        course_ref_num = course_list[course]["courseReferenceNumber"]
        params["courseReferenceNumber"] = course_ref_num       
        
        try:
            response = requests.post(url, headers=headers, params=params)
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch course description: {e}")
        
        if response.status_code == 200:
            # Parse the HTML response to extract course description
            soup = BeautifulSoup(response.text, "html.parser")
            description_section = soup.find("section", {"aria-labelledby": "courseDescription"})
            
            if description_section:
                # Extract and clean up the text
                description_text = description_section.get_text(strip=True)
                course_list[course]["course_description"] = description_text
            else:
                course_list[course]["course_description"] = "No description available."
                logging.warning(f"No description found for course: {course_ref_num}")
        else:
            # Handle cases where the request was unsuccessful
            course_list[course]["course_description"] = "Failed to fetch description."
            logging.error(f"Failed to fetch description for course: {course_ref_num}")

    return course_list

def dump_to_csv(course_data, **context):
    output_path = context['dag_run'].conf.get('output_path', '/tmp/banner_data')
    
    file_path = os.path.join(output_path, "banner_course_data.csv")
    
    # Check if the file exists
    if os.path.exists(file_path):
        # Append the data to the file
        with open(file_path, "a") as file:
            writer = csv.writer(file)
            for course in course_data:
                writer.writerow(course_data[course].values())
    else:
        # Create a new file and write the data
        with open(file_path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["crn", "course_title", "subject_course", "faculty_name", "campus_description", "course_description"])
            for course in course_data:
                writer.writerow(course_data[course].values())