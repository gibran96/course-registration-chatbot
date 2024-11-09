import re
import string


semester_map = {
    "10": "Fall",
    "30": "Spring",
    "40": "Summer 1",
    "50": "Summer Full",
    "60": "Summer 2"
}

def remove_punctuation(text):
    """
    Remove all punctuation from a given text.

    Parameters
    ----------
    text : str
        The text from which to remove punctuation.

    Returns
    -------
    str
        The input text with all punctuation removed.
    """
    punts = string.punctuation
    new_text = ''.join(e for e in text if e not in punts)
    return new_text

def clean_response(response):
    """Remove leading index numbers from a response."""
    response = re.sub(r"^\d+\s*", "", response.strip())
    # remove next line characters
    response = response.replace("\n", " ")
    response = response.replace("\r", " ")
    response = response.replace("\t", " ")
    return response

def clean_text(text):
    """Clean and standardize text."""
    text = text.strip()
    text = ''.join(e for e in text if e.isalnum() or e.isspace() or e in string.punctuation)
    text = text.lower()
    return text

def check_unknown_text(reviews_df, courses_df):
    """
    Checks for unknown text in the reviews and courses DataFrames.
    Args:
        reviews_df (pandas.DataFrame): DataFrame containing reviews data.
        courses_df (pandas.DataFrame): DataFrame containing courses data.
    Returns:
        tuple: A tuple containing:
            - unknown_reviews (pandas.DataFrame): DataFrame containing reviews with unknown text.
            - unknown_courses (pandas.DataFrame): DataFrame containing courses with unknown text.
    """
    # Check for unknown text in reviews
    unknown_reviews = reviews_df[reviews_df["question"] == "unknown"]
    
    # Check for unknown text in courses
    unknown_courses = courses_df[courses_df["crn"] == ""]
    
    return unknown_reviews, unknown_courses

# Function to get the days of the course lecture
def get_days(meeting_time):
    """
    Extracts and returns the days of the week a course is held.

    Args:
        meeting_time (dict or tuple): A dictionary where the keys are the days of the week and the values
            are booleans indicating whether the course is held on that day. If a tuple is provided, it is
            assumed to contain a single dictionary element.

    Returns:
        str: A string containing the days of the week the course is held, separated by semicolons.

    """
    if isinstance(meeting_time, tuple):
        meeting_time = meeting_time[0]
    days = [day for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'] 
            if meeting_time.get(day, False)]
    return ";".join(days)

# TODO - Implement this function
def get_next_term(cookie_output):
    pass

                
                
# Function to get the semester name from the term
def get_semester_name(term):
    """
    Extracts and returns the semester name from a given term code.

    Args:
        term (str): A term code string where the last two characters represent the semester 
                    and the first four characters represent the year.

    Returns:
        str: The semester name followed by the year (e.g., "Fall 2023").

    Raises:
        KeyError: If the extracted semester code is not found in the semester_map.
    """
    semester = term[-2:]
    return semester_map[semester] + " " + term[:4]