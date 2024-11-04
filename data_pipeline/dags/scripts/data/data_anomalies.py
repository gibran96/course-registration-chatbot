import pandas as pd


def check_for_gender_bias(df, column_name):
    """
    Check for gender bias in the specified column of the DataFrame by identifying and replacing gender-specific pronouns.
    Args:
        df (pd.DataFrame): The DataFrame containing the data to be checked.
        column_name (str): The name of the column in the DataFrame to check for gender-specific pronouns.
    Returns:
        tuple: A tuple containing:
            - flag (bool): True if any gender-specific pronouns were found and replaced, False otherwise.
            - sensitive_data_found (pd.DataFrame): A DataFrame containing the rows where gender-specific pronouns were found, with columns ["crn", "question", "response"].
    """
    # Check for gender bias in the df for the given column
    # Check for any gender specific pronouns in the responses and replace it with the professor.

    gender_sensitive_pronouns = [" he " , " him ", " his ", " she ", " her ", " hers ", " they ", " them ", " theirs "]

    flag = False

    sensitive_data_found = pd.DataFrame(columns=["crn", "question", "response"])
    # Keep a track of all the rows that have the gender specific pronouns
    # Check each row in the specified column for gender-specific pronouns
    for index, row in df.iterrows():
        response_text = f" {row[column_name]} "  # Adding spaces around the text to handle word boundaries
        found_pronouns = any(pronoun in response_text.lower() for pronoun in gender_sensitive_pronouns)
        
        if found_pronouns:
            # Set flag to True if any sensitive data is found
            flag = True
            
            # Append row to sensitive_data_found DataFrame
            sensitive_data_found[-1] = [row["crn"], row["question"], row[column_name]]
            
            # Replace pronouns with "the professor" in the response text
            for pronoun in gender_sensitive_pronouns:
                response_text = response_text.replace(pronoun, " the professor ")
            
            # Update the original DataFrame with the modified text
            df.at[index, column_name] = response_text.strip()
    
    return flag, sensitive_data_found