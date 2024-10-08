# Extract data from a structured pdf file

# Each pdf file contains a table with the following sections:
#   Initially it has information about the course, such as the course code, title, and description.

#  Then it has a table with the following sections:
#  It has responses from multiple students to some questions about the course.
#  each question starts with Q: followed by the question and then the responses from the students.
# The responses are indexed by number, followed by the response text.

# The goal is to extract the responses from the students and store them in a structured format.
# The structured format is a dictionary with the following keys:
#   course_code
#   course_title
#   course_description
#   responses

# The responses key will have a list of dictionaries, where each dictionary has the following keys:
#   question
#   responses
# The responses key will have a list of responses from the students.

# The function extract_data_from_pdf takes a file path as input and returns the structured data.
# The function should read the pdf file and extract the data as described above.

# You can use the PyMuPDF library to read the pdf file.
# You can install the library using the following command:
# pip install pymupdf

import os
import pymupdf
import json
import pandas as pd

question_csv_files = {
    "what were the strengths of this course andor this instructor": "Q1.csv",
    "what could the instructor do to make this course better": "Q2.csv",
    "please expand on the instructors strengths andor areas for improvement in facilitating inclusive learning": "Q3.csv",
    "please comment on your experience of the online course environment in the openended text box": "Q4.csv"
}

# Initialize DataFrames for each question
dataframes = {
    "what were the strengths of this course andor this instructor": pd.DataFrame(columns=["crn", "response", "instructor"]),
    "what could the instructor do to make this course better": pd.DataFrame(columns=["crn", "response", "instructor"]),
    "please expand on the instructors strengths andor areas for improvement in facilitating inclusive learning": pd.DataFrame(columns=["crn", "response", "instructor"]),
    "please comment on your experience of the online course environment in the openended text box": pd.DataFrame(columns=["crn", "response", "instructor"])
}

def extract_data_from_pdf(file_path):
    # Open the pdf file
    pdf_file = pymupdf.open(file_path)

    # Get the number of pages in the pdf
    num_pages = len(pdf_file)

    # Initialize the structured data dictionary
    structured_data = {
        "crn": "",
        "course_title": "",
        "responses": []
    }

    # Loop through each page of the pdf
    for page_num in range(num_pages):
        # Get the text content of the page
        page_text = pdf_file[page_num].get_text()

        # Check if the page contains course information
        if "Course ID" in page_text:
            # Extract course code, title, and description
            instructor = page_text.split("Instructor: ")[1].split("\n")[0]
            course_code = page_text.split("Course ID: ")[1].split("\n")[0]
            course_title = page_text.split("\n")[0].split("(")[0].strip()
            # course_description = page_text.split("Course Description: ")[1].split("\n")[0]

            structured_data["crn"] = course_code
            structured_data["instructor"] = instructor
            structured_data["course_title"] = course_title
            # structured_data["course_title"] = course_title
            # structured_data["course_description"] = course_description

        # Check if the page contains responses
        if "Q:" in page_text:
            # Extract questions and responses
            questions = page_text.split("Q: ")[1:]
            for question in questions:
                question_text = question.split("\n")[0]
                responses = question.split("\n")[1:]
                actual_responses = []
                temp_response = ""
                for response in responses:
                    if response.isdigit():
                        
                        actual_responses.append(temp_response)
                        temp_response = ""
                    else:
                        temp_response += response

                structured_data["responses"].append({
                    "question": question_text,
                    "responses": actual_responses
                })

    # Close the pdf file
    pdf_file.close()
    # print(json.dumps(structured_data, indent=4))

    # Use nltk to clean the text
    for response in structured_data["responses"]:
        response["question"] = clean_text(response["question"])
        response["responses"] = [clean_text(r) for r in response["responses"]]
    return structured_data

def clean_text(text):
    # Remove leading and trailing whitespaces
    text = text.strip()
    # Remove special characters
    text = ''.join(e for e in text if e.isalnum() or e.isspace())
    # Convert to lowercase
    text = text.lower()
    return text

def process_dataframes(structured_data):
    crn = structured_data["crn"]
    instructor = structured_data["instructor"]
    
    for response_data in structured_data["responses"]:
        question = response_data.get("question", "")
        responses = response_data.get("responses", [])
        
        if question in dataframes:
            for response in responses:
                if response:  # Ignore empty responses
                    # Add response to the DataFrame for the corresponding question
                    new_row = {"crn": crn, "response": response, "instructor": instructor}
                    dataframes[question] = dataframes[question]._append(new_row, ignore_index=True)

def save_dataframes_to_csv():
    """
    Save each DataFrame to its respective CSV file.
    """
    for question, df in dataframes.items():
        csv_filename = question_csv_files[question]
        # If the CSV already exists, append without headers, otherwise create a new CSV
        if os.path.exists(csv_filename):
            df.to_csv(csv_filename, mode='a', header=False, index=False)
        else:
            df.to_csv(csv_filename, index=False)


def process_pdfs(pdf_directory):
    for pdf_file in os.listdir(pdf_directory):
        if pdf_file.endswith('.pdf'):
            pdf_path = os.path.join(pdf_directory, pdf_file)
            courseId = pdf_file.split('.')[0]
            print(f"Processing: {pdf_file}")
            
            # Extract and parse text from the PDF
            structured_data = extract_data_from_pdf(pdf_path)
            file_path = "data/course_comments_text/{courseId}.txt".format(courseId=courseId)
            with open(file_path, "w") as f:
                f.write(json.dumps(structured_data, indent=4))

file_path = "data/course_comments"

process_pdfs(file_path)