import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.banner.fetch_banner_data import dump_to_csv, get_cookies, get_course_description, get_course_prerequisites, get_courses_list, get_faculty_info


mock_html = """<h3>Catalog Prerequisites</h3>

    <table class="basePreqTable">
      <thead>
      <tr>
        <th>And/Or</th>
        <th></th>
        <th>Test</th>
        <th>Score</th>
        <th>Subject</th>
        <th>Course Number</th>
        <th>Level</th>
        <th>Grade</th>
        <th></th>
      </tr>
      </thead>
      <tbody>

        <tr>

          <td></td>

          <td>(</td>
          <td></td>
          <td></td>
          <td>Mathematics</td>
          <td>2321</td>
          <td>Undergraduate</td>
          <td>C-</td>
          <td></td>
        </tr>

        </tbody>
        </table>"""

class TestBannerDataPipeline(unittest.TestCase):

    @patch("requests.post")
    @patch("airflow.models.Variable.get")
    def test_get_cookies_success(self, mock_var_get, mock_post):
        # Mock context and Variable retrieval
        mock_context = {'dag_run': MagicMock(), 'ti': MagicMock()}
        mock_context['dag_run'].conf.get.return_value = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
        mock_var_get.return_value = "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
        
        # Mock a successful POST response with required headers
        mock_response = MagicMock()
        mock_response.headers = {
            "Set-Cookie": "dummycookie; Path=/; Path=/; Secure, nubanner-cookie"
        }
        mock_post.return_value = mock_response

        # Run function
        get_cookies(**mock_context)
        
        # Verify XCom push call
        mock_context['ti'].xcom_push.assert_called_with(
            key='cookie_output',
            value={
                "cookie": "dummycookie",    
                "jsessionid": "dummycookie",
                "nubanner_cookie": "nubanner-cookie",
                "base_url": "https://nubanner.neu.edu/StudentRegistrationSsb/ssb/"
            }
        )

    @patch("requests.get")
    @patch("airflow.models.Variable.get")
    def test_get_courses_list_success(self, mock_get_semester_name, mock_get):
        # Mock context and XCom pull
        mock_context = {'ti': MagicMock()}
        mock_context['ti'].xcom_pull.return_value = {
            "cookie": "cookie=value",
            "jsessionid": "jsessionid",
            "nubanner_cookie": "nubanner_cookie",
            "base_url": "http://test-url.com"
        }
        
        # Mock a successful GET response with JSON data
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"courseReferenceNumber": "12345", "campusDescription": "Main Campus", "courseTitle": "Intro to CS", "subjectCourse": "CS101"}
            ]
        }
        mock_get.return_value = mock_response
        
        # Run function
        get_courses_list(**mock_context)
        
        # Verify XCom push for course list DataFrame
        expected_df = pd.DataFrame([{
            "crn": "12345",
            "campus_description": "Main Campus",
            "course_title": "Intro to CS",
            "subject_course": "CS101",
            "faculty_name": "",
            "course_description": "",
            "term": "Spring 2025",
            "begin_time": "",
            "end_time": "",
            "days": "",
            "prereq": ""
        }])
        pd.testing.assert_frame_equal(mock_context['ti'].xcom_push.call_args[1]["value"], expected_df)

    @patch("requests.post")
    def test_get_faculty_info_success(self, mock_post):
        # Mock cookie_output and course_list_df
        cookie_output = {"cookie": "cookie=value", "jsessionid": "jsessionid", "nubanner_cookie": "nubanner_cookie", "base_url": "http://test-url.com"}
        course_list_df = pd.DataFrame([{"crn": "12345", "faculty_name": ""}])

        # Mock a successful POST response with faculty data
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "fmt": [{
                "faculty": [{"displayName": "Dr. John Doe"}],
                "meetingTime": {"beginTime": "09:00", "endTime": "10:30"}
            }]
        }
        mock_post.return_value = mock_response
        
        # Run function
        updated_df = get_faculty_info(cookie_output, course_list_df)
        
        # Check if faculty information was correctly added
        self.assertEqual(updated_df.loc[0, "faculty_name"], "Dr. John Doe")
        self.assertEqual(updated_df.loc[0, "begin_time"], "09:00")
        self.assertEqual(updated_df.loc[0, "end_time"], "10:30")

    @patch("requests.post")
    @patch("bs4.BeautifulSoup")
    def test_get_course_description_success(self, mock_bs, mock_post):
        # Mock cookie_output and course_list_df
        cookie_output = {"cookie": "cookie=value", "jsessionid": "jsessionid", "nubanner_cookie": "nubanner_cookie", "base_url": "http://test-url.com"}
        course_list_df = pd.DataFrame([{"crn": "12345"}])

        # Mock a successful POST response and BeautifulSoup parsing
        mock_response = MagicMock()
        mock_response.text = "<html><section aria-labelledby='courseDescription'>Course description here.</section></html>"
        mock_post.return_value = mock_response
        mock_bs.return_value.find.return_value.get_text.return_value = "Course description here."
        
        # Run function
        updated_df = get_course_description(cookie_output, course_list_df)
        
        # Check if course description was correctly added
        self.assertEqual(updated_df.loc[0, "course_description"], "Course description here.")

    @patch("requests.post")
    @patch("bs4.BeautifulSoup")
    def test_get_course_prerequisites_success(self, mock_bs, mock_post):
        # Mock cookie_output and course_list_df
        cookie_output = {"cookie": "cookie=value", "jsessionid": "jsessionid", "nubanner_cookie": "nubanner_cookie", "base_url": "http://test-url.com"}
        course_list_df = pd.DataFrame([{"crn": "12345"}])

        # Mock a successful POST response and BeautifulSoup parsing
        mock_response = MagicMock()
        mock_response.text = mock_html
        mock_post.return_value = mock_response
        mock_bs.return_value.find.return_value.find_all.return_value = [MagicMock(text="CS5100")]

        # Run function
        updated_df = get_course_prerequisites(cookie_output, course_list_df)
        
        # Check if prerequisites were correctly added
        self.assertEqual(updated_df.loc[0, "prereq"], [{"and_or": "", "subject": "Mathematics", "course_number": "2321", "level": "Undergraduate", "grade": "C-"}])

    @patch("pandas.DataFrame.to_csv")
    def test_dump_to_csv_empty_check(self, mock_to_csv):
        # Mock context and DataFrame
        mock_context = {'ti': MagicMock()}
        mock_context['ti'].xcom_pull.return_value = pd.DataFrame()

        with self.assertRaises(ValueError):
            dump_to_csv(**mock_context)  # Should raise ValueError when DataFrame is empty
