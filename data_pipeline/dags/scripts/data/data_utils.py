import re
import string

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