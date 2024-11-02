import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import patch, Mock, mock_open, MagicMock
import requests
import airflow
import ast
import csv

from scripts.fetch_banner_data import get_cookies
from scripts.fetch_banner_data import get_courses_list
from scripts.fetch_banner_data import get_course_description
from scripts.fetch_banner_data import dump_to_csv



# Assuming get_cookies is defined here or imported

@patch("requests.post")
@patch("airflow.models.Variable.get")
def test_get_cookies_successful_response(mock_variable_get, mock_post):
    # Mock the context with dag_run conf
    mock_context = {
        'dag_run': Mock(conf={"get": Mock(return_value="https://nubanner.neu.edu/StudentRegistrationSsb/ssb/")})
    }
    # Mock Variable.get if base_url is not set in dag_run.conf
    mock_variable_get.return_value = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"

    # Mock response for a successful request
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {}
    mock_response.headers = {
        "Set-Cookie": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604; Path=/; Path=/; Secure, nubanner-cookie=2384404891.36895.0000"
    }
    mock_post.return_value = mock_response

    # Call the function with the mock context
    result = get_cookies(**mock_context)

    # Expected output
    expected_output = {
        "cookie": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "jsessionid": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "nubanner_cookie": "nubanner-cookie=2384404891.36895.0000",
        "base_url": "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    }

    # Assertions
    assert result == expected_output
    mock_post.assert_called_once()  # Ensure request was made


@patch("requests.post")
@patch("airflow.models.Variable.get")
def test_get_cookies_error_in_response_json(mock_variable_get, mock_post, caplog):
    # Mock the context with dag_run conf
    mock_context = {
        'dag_run': Mock(conf={"get": Mock(return_value="https://nubanner.neu.edu/StudentRegistrationSsb/ssb/")})
    }
    # Mock Variable.get if base_url is not set in dag_run.conf
    mock_variable_get.return_value = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"

    # Mock response with "regAllowed" key, indicating an error
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"regAllowed": False}
    mock_post.return_value = mock_response

    # Call the function with the mock context and capture logs
    with caplog.at_level("ERROR"):
        result = get_cookies(**mock_context)

    # Assert that the function returns None and logs the error
    assert result is None
    assert "Error in fetching cookies" in caplog.text


@patch("requests.post")
@patch("airflow.models.Variable.get")
def test_get_cookies_request_exception(mock_variable_get, mock_post, caplog):
    # Mock the context with dag_run conf
    mock_context = {
        'dag_run': Mock(conf={"get": Mock(return_value="https://nubanner.neu.edu/StudentRegistrationSsb/ssb/")})
    }
    # Mock Variable.get if base_url is not set in dag_run.conf
    mock_variable_get.return_value = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"

    # Simulate a RequestException
    mock_post.side_effect = requests.exceptions.RequestException("Network error")

    # Call the function with the mock context and capture logs
    with caplog.at_level("ERROR"):
        result = get_cookies(**mock_context)

    # Assert that the function returns None and logs the exception
    assert result is None
    assert "Failed to fetch cookies" in caplog.text
    
@patch("requests.get")
def test_get_courses_list_successful_response(mock_get):
    # Mocking the cookie output
    cookie_output = str({
        "cookie": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "jsessionid": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "nubanner_cookie": "nubanner-cookie=2384404891.36895.0000",
        "base_url": "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    })

    # Mock response data
    mock_response_data = {
        "totalCount": 1,
        "data": [
            {
                "courseReferenceNumber": "12345",
                "campusDescription": "Main Campus",
                "courseTitle": "Introduction to Computer Science",
                "subjectCourse": "CS101",
                "faculty": [{"displayName": "Dr. John Doe"}],
                "meetingsFaculty": [{"meetingTime": {"beginTime": "10:00", "endTime": "11:00", "monday": True, "tuesday": False, "wednesday": True, "thursday": False, "friday": True, "saturday": False, "sunday": False}}]
            }
        ]
    }

    # Mock response for a successful request
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_data
    mock_get.return_value = mock_response

    # Expected output
    expected_output = {
        "12345": {
            "crn": "12345",
            "campus_description": "Main Campus",
            "course_title": "Introduction to Computer Science",
            "subject_course": "CS101",
            "faculty_name": "Dr. John Doe",
            "term": "Spring 2025",
            "begin_time": "10:00",
            "end_time": "11:00",
            "days": "monday;wednesday;friday"
        }
    }

    # Call the function
    result = get_courses_list(cookie_output)

    # Assertions
    assert result == expected_output
    mock_get.assert_called_once()  # Ensure the request was made


@patch("requests.get")
def test_get_courses_list_empty_faculty(mock_get):
    # Mocking the cookie output
    cookie_output = str({
        "cookie": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "jsessionid": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "nubanner_cookie": "nubanner-cookie=2384404891.36895.0000",
        "base_url": "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    })

    # Mock response data with empty faculty list
    mock_response_data = {
        "totalCount": 1,
        "data": [
            {
                "courseReferenceNumber": "12345",
                "campusDescription": "Main Campus",
                "courseTitle": "Introduction to Computer Science",
                "subjectCourse": "CS101",
                "faculty": [],
                "meetingsFaculty": [{"meetingTime": {"beginTime": "10:00", "endTime": "11:00", "monday": True, "tuesday": False, "wednesday": True, "thursday": False, "friday": True, "saturday": False, "sunday": False}}]
            }
        ]
    }

    # Mock response for a successful request
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_data
    mock_get.return_value = mock_response

    # Expected output with empty faculty_name
    expected_output = {
        "12345": {
            "crn": "12345",
            "campus_description": "Main Campus",
            "course_title": "Introduction to Computer Science",
            "subject_course": "CS101",
            "faculty_name": "",
            "term": "Spring 2025",
            "begin_time": "10:00",
            "end_time": "11:00",
            "days": "monday;wednesday;friday"
        }
    }

    # Call the function
    result = get_courses_list(cookie_output)

    # Assertions
    assert result == expected_output
    mock_get.assert_called_once()  # Ensure the request was made


@patch("requests.get")
def test_get_courses_list_request_exception(mock_get, caplog):
    # Mocking the cookie output
    cookie_output = str({
        "cookie": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "jsessionid": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "nubanner_cookie": "nubanner-cookie=2384404891.36895.0000",
        "base_url": "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    })

    # Simulate a RequestException
    mock_get.side_effect = requests.exceptions.RequestException("Network error")

    # Capture logs
    with caplog.at_level("ERROR"):
        result = get_courses_list(cookie_output)

    # Assert that the function returns an empty list and logs the exception
    assert result == []
    assert "Failed to fetch course list" in caplog.text
    
    
@patch("requests.post")
@patch("bs4.BeautifulSoup")
@patch("data_pipeline.dags.scripts.extract_trace_data.clean_response")
def test_get_course_description_successful(mock_clean_response, mock_soup, mock_post):
    # Mock the cookie and course list inputs
    cookie_output = str({
        "cookie": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "jsessionid": "JSESSIONID=0BE4FE8240151D9C328E183AA883C604",
        "nubanner_cookie": "nubanner-cookie=2384404891.36895.0000",
        "base_url": "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
    })
    course_list = str({
        "12345": {
            "crn": "12345",
            "campus_description": "Main Campus",
            "course_title": "Intro to CS",
            "subject_course": "CS101",
            "faculty_name": "Dr. John Doe",
            "term": "202530"
        }
    })

    # Mock successful response with HTML content for course description
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<section aria-labelledby='courseDescription'>Intro to CS course description</section>"
    mock_post.return_value = mock_response

    # Mock BeautifulSoup to find the description section
    mock_soup.return_value.find.return_value.get_text.return_value = "Intro to CS course description"
    mock_clean_response.return_value = "Intro to CS course description"

    # Expected result
    expected_output = {
        "12345": {
            "crn": "12345",
            "campus_description": "Main Campus",
            "course_title": "Intro to CS",
            "subject_course": "CS101",
            "faculty_name": "Dr. John Doe",
            "term": "202530",
            "course_description": "Intro to CS course description"
        }
    }

    # Call the function
    result = get_course_description(cookie_output, course_list)

    # Assertions
    assert result == expected_output
    mock_post.assert_called_once() 
