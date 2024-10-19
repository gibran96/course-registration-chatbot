# Use Banner API to pull course information from Banner
# Get course information for all courses in the current semester
# Save the data to a csv file


import requests
import json
import csv
import os
import sys
import time
import datetime
import pandas as pd
import numpy as np

# Load environment variables

base_url = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"

def get_courses_list():
    """ 
        Get a list of all courses in the current semester
    """
    
        # Get the cookie, JSESSIONID and nubanner-cookie
    cookie, jsessionid, nubanner_cookie = get_cookies()
        
    # Send a GET request to Banner API @ searchResults/searchResults to get the list of all courses in the current semester
    url = base_url + "searchResults/searchResults"

    headers = {
        "Cookie": jsessionid+"; "+nubanner_cookie
    }

    # Example URI
    
    # Add the query parameters to the URL using requests library
    params = {
        "txt_subject": "CS",
        "txt_courseNumber": "",
        "txt_term": "202510",
        "startDatepicker": "",
        "endDatepicker": "",
        "pageOffset": 0,
        "pageMaxSize": 500,
        "sortColumn": "subjectDescription",
        "sortDirection": "asc"
    }
    response = requests.get(url, headers=headers, params=params)


    # Get the JSON response
    response_json = response.json()

    print("Number of courses: ", response_json["totalCount"])

    return response_json



def get_cookies():
    """ Get the cookie, JSESSIONID and nubanner-cookie from the response """

    # Send a POST request to Banner API @ /term/search to get the cookie, JSESSIONID and nubanner-cookie
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

    response = requests.post(url, headers=headers, params=payload)

    print("Response: ", response.text)

    # Get the cookie from the response
    cookie = response.headers["Set-Cookie"]
    cookie = cookie.split(";")[0]

    # print("Cookie: ", cookie)

    # Get the JSESSIONID from the response
    jsessionid_ = response.headers["Set-Cookie"]
    jsessionid = jsessionid_.split(";")[0]

    # print("JSESSIONID: ", jsessionid)

    # Get the nubanner-cookie from the response
    nubanner_cookie = response.headers["Set-Cookie"].split(";")[3].split(", ")[1]

    return cookie, jsessionid, nubanner_cookie


def filter_courses(all_courses_info):
    """ Store this information for all the courses

        
        courseReferenceNumber
        campusDescription
        courseTitle
        subjectCourse
        facultyName
        
    """

    course_data = {}

    for course in all_courses_info["data"]:
        course_data[course["courseReferenceNumber"]] = {
            "courseReferenceNumber": course["courseReferenceNumber"],
            "campusDescription": course["campusDescription"],
            "courseTitle": course["courseTitle"],
            "subjectCourse": course["subjectCourse"],
            "facultyName": course["faculty"][0]["displayName"]
        }

    return course_data

course_data = filter_courses(get_courses_list())

def dump_to_csv(course_data):
    """ Dump the course data to a csv file """

    # Check if the file exists
    if os.path.exists("course_data.csv"):
        # Append the data to the file
        with open("course_data.csv", "a") as file:
            writer = csv.writer(file)
            for course in course_data:
                writer.writerow(course_data[course].values())
    else:
        # Create a new file and write the data
        with open("course_data.csv", "w") as file:
            writer = csv.writer(file)
            writer.writerow(["courseReferenceNumber", "campusDescription", "courseTitle", "subjectCourse", "facultyName"])
            for course in course_data:
                writer.writerow(course_data[course].values())

dump_to_csv(course_data)