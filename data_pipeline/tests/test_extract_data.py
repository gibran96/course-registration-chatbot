import sys
import os

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))


import pandas as pd

import pytest
from unittest.mock import MagicMock

from dags.scripts.extract_data import clean_text
from dags.scripts.extract_data import clean_response

from dags.scripts.extract_data import process_data
from dags.scripts.extract_data import extract_data_from_pdf

class TestCleanResponse:
    """Test suite for the `clean_response` function."""

    def test_with_leading_number(self):
        response = "2 Very engaging, encouraging, and knowledgeable."
        assert clean_response(response) == "Very engaging, encouraging, and knowledgeable."

    def test_with_leading_number_and_spaces(self):
        response = "  2   Very engaging, encouraging, and knowledgeable.  "
        assert clean_response(response) == "Very engaging, encouraging, and knowledgeable."

    def test_without_leading_number(self):
        response = "Very engaging, encouraging, and knowledgeable."
        assert clean_response(response) == "Very engaging, encouraging, and knowledgeable."

    def test_empty_string(self):
        response = ""
        assert clean_response(response) == ""

    def test_only_whitespace(self):
        response = "    "
        assert clean_response(response) == ""
        
        
class TestCleanText:
    """Test suite for the `clean_text` function."""

    def test_with_leading_and_trailing_spaces(self):
        text = "   2 Very engaging, encouraging, and knowledgeable.   "
        assert clean_text(text) == "2 very engaging, encouraging, and knowledgeable."

    def test_with_mixed_case_letters(self):
        text = "2 Very Engaging, Encouraging, and Knowledgeable."
        assert clean_text(text) == "2 very engaging, encouraging, and knowledgeable."

    def test_with_only_spaces(self):
        text = "     "
        assert clean_text(text) == ""

    def test_with_empty_string(self):
        text = ""
        assert clean_text(text) == ""
        

# Mock question map used inside the function
question_map = {
    "What were the strengths of this course and/or this instructor?": "instructor_strengths",
    "What could the instructor do to make this course better?": "instructor_improvement",
    "Please expand on the instructor's strengths and/or areas for improvement in facilitating inclusive learning.": "instructor_inclusive_learning"
}

class TestExtractDataFromPDF:
    """Test suite for the `extract_data_from_pdf` function."""

    @pytest.fixture
    def mock_pdf_file(self):
        """Fixture to mock a PDF file with pages."""
        page1 = MagicMock()
        page1.get_text.return_value = (
            "Bias/Ethics Implications in AI (Spring 2024)\n"
            "Course ID: 41457\n"
            "Instructor: Alikhani, Malihe\n"
            "Catalog & Section: 6983 04\n"
            "Q: What were the strengths of this course and/or this instructor?\n"
            "Very engaging, encouraging, and knowledgeable. Very understanding and helpful\n"
            "Q: What could the instructor do to make this course better?\n"
            "I enjoyed the live guest lectures-- I would love to have more incorporated throughout the semester."
        )

        page2 = MagicMock()
        page2.get_text.return_value = (
            "Q: What were the strengths of this course and/or this instructor?\n"
            "Prof Malihe is very approachable and her way of explaining things makes very nuanced topics simple to understand. This made the course very interesting as it could have gotten very monotonous to study ethics and biases in AI.\n"
            "Q: What could the instructor do to make this course better?\n"
            "Perhaps from assignments?"
        )

        return [page1, page2]


    def test_extract_complete_data(self, mock_pdf_file):
        """Test extraction with complete data from multiple pages."""
        
        structured_data = extract_data_from_pdf(mock_pdf_file)
        print("Structured Data:", structured_data)

        assert structured_data["crn"] == "41457"
        assert structured_data["instructor"] == "Alikhani, Malihe"
        assert structured_data["course_code"] == "6983"
        assert structured_data["course_title"] == "Bias/Ethics Implications in AI"
        
        assert len(structured_data["responses"]) == 4
        assert structured_data["responses"][0]["question"] == "Q1"
        assert structured_data["responses"][0]["responses"] == ['very engaging, encouraging, and knowledgeable. very understanding and helpful']
        assert structured_data["responses"][1]["question"] == "Q2"
        assert structured_data["responses"][1]["responses"] == ['i enjoyed the live guest lectures-- i would love to have more incorporated throughout the semester.']
        assert structured_data["responses"][2]["question"] == "Q1"
        assert structured_data["responses"][2]["responses"] == ['prof malihe is very approachable and her way of explaining things makes very nuanced topics simple to understand. this made the course very interesting as it could have gotten very monotonous to study ethics and biases in ai.']
        assert structured_data["responses"][3]["question"] == "Q2"
        assert structured_data["responses"][3]["responses"] ==['perhaps from assignments?']


    def test_extract_with_empty_pdf(self):
        """Test extraction when the PDF file is empty."""
        structured_data = extract_data_from_pdf([])

        print("Structured Data:", structured_data)

        assert structured_data["crn"] == ""
        assert structured_data["instructor"] == ""
        assert structured_data["course_code"] == ""
        assert structured_data["course_title"] == ""
        assert structured_data["responses"] == []
        
        
class TestProcessData:
    """Test suite for the `process_data` function."""

    @pytest.fixture
    def structured_data(self):
        """Fixture to provide sample structured data matching the new format."""
        return {
            "crn": "41457",
            "course_code": "6983",
            "course_title": "Bias/Ethics Implications in AI",
            "instructor": "Alikhani, Malihe",
            "responses": [
                {
                    "question": "Q1",
                    "responses": [
                        "Very engaging, encouraging, and knowledgeable. Very understanding and helpful"
                    ]
                },
                {
                    "question": "Q2",
                    "responses": [
                        "I enjoyed the live guest lectures-- I would love to have more incorporated throughout the semester."
                    ]
                },
                {
                    "question": "Q1",
                    "responses": [
                        "Prof Malihe is very approachable and her way of explaining things makes very nuanced topics simple to understand. This made the course very interesting as it could have gotten very monotonous to study ethics and biases in AI."
                    ]
                },
                {
                    "question": "Q2",
                    "responses": ["Perhaps from assignments?"]
                }
            ]
        }

    @pytest.fixture
    def empty_dataframes(self):
        """Fixture to provide empty courses and reviews DataFrames."""
        courses_df = pd.DataFrame(columns=["crn", "course_code", "course_title", "instructor"])
        reviews_df = pd.DataFrame(columns=["review_id", "crn", "question", "response"])
        return reviews_df, courses_df

    def test_process_data_new_course(self, structured_data, empty_dataframes):
        """Test processing data with a new course."""
        reviews_df, courses_df = empty_dataframes
        updated_reviews_df, updated_courses_df = process_data(structured_data, reviews_df, courses_df)

        # Verify course added to courses_df
        assert len(updated_courses_df) == 1
        assert updated_courses_df.iloc[0]["crn"] == "41457"
        assert updated_courses_df.iloc[0]["course_code"] == "6983"
        assert updated_courses_df.iloc[0]["course_title"] == "Bias/Ethics Implications in AI"
        assert updated_courses_df.iloc[0]["instructor"] == "Alikhani, Malihe"

        # Verify reviews added to reviews_df
        assert len(updated_reviews_df) == 4
        assert updated_reviews_df.iloc[0]["response"] == "Very engaging, encouraging, and knowledgeable. Very understanding and helpful"
        assert updated_reviews_df.iloc[1]["response"] == "I enjoyed the live guest lectures-- I would love to have more incorporated throughout the semester."
        assert updated_reviews_df.iloc[2]["response"] == "Prof Malihe is very approachable and her way of explaining things makes very nuanced topics simple to understand. This made the course very interesting as it could have gotten very monotonous to study ethics and biases in AI."
        assert updated_reviews_df.iloc[3]["response"] == "Perhaps from assignments?"

    def test_process_data_existing_course(self, structured_data):
        """Test processing data with an existing course."""
        # Create a courses_df with the same CRN
        courses_df = pd.DataFrame([{
            "crn": "41457",
            "course_code": "6983",
            "course_title": "Bias/Ethics Implications in AI",
            "instructor": "Alikhani, Malihe"
        }])
        reviews_df = pd.DataFrame(columns=["review_id", "crn", "question", "response"])

        updated_reviews_df, updated_courses_df = process_data(structured_data, reviews_df, courses_df)

        # Verify no new course row added
        assert len(updated_courses_df) == 1

        # Verify reviews added to reviews_df
        assert len(updated_reviews_df) == 4

    def test_process_data_empty_responses(self, structured_data):
        """Test processing data with no responses."""
        structured_data["responses"] = []  # No responses in structured data
        reviews_df = pd.DataFrame(columns=["review_id", "crn", "question", "response"])
        courses_df = pd.DataFrame(columns=["crn", "course_code", "course_title", "instructor"])

        updated_reviews_df, updated_courses_df = process_data(structured_data, reviews_df, courses_df)

        # Verify course added to courses_df
        assert len(updated_courses_df) == 1

        # Verify no reviews added to reviews_df
        assert len(updated_reviews_df) == 0

    def test_process_data_multiple_courses(self):
        """Test processing multiple different courses."""
        structured_data_1 = {
            "crn": "41457",
            "course_code": "6983",
            "course_title": "Bias/Ethics Implications in AI",
            "instructor": "Alikhani, Malihe",
            "responses": [
                {"question": "Q1", "responses": ["Amazing course!"]}
            ]
        }

        structured_data_2 = {
            "crn": "67890",
            "course_code": "5200",
            "course_title": "Data Science",
            "instructor": "Dr. Johnson",
            "responses": [
                {"question": "Q1", "responses": ["Very informative."]}
            ]
        }

        reviews_df = pd.DataFrame(columns=["review_id", "crn", "question", "response"])
        courses_df = pd.DataFrame(columns=["crn", "course_code", "course_title", "instructor"])

        # Process first course
        reviews_df, courses_df = process_data(structured_data_1, reviews_df, courses_df)
        # Process second course
        reviews_df, courses_df = process_data(structured_data_2, reviews_df, courses_df)

        # Verify both courses are added
        assert len(courses_df) == 2
        assert set(courses_df["crn"]) == {"41457", "67890"}

        # Verify reviews added for both courses
        assert len(reviews_df) == 2
        assert reviews_df.iloc[0]["response"] == "Amazing course!"
        assert reviews_df.iloc[1]["response"] == "Very informative."
        
