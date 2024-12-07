import logging
import streamlit as st
import uuid
import requests

# Backend URL (replace with your backend endpoint)

if 'base_url' not in st.session_state:
    st.session_state.base_url = "https://course-compass-542057411868.us-east1.run.app/"

if 'backend_url' not in st.session_state:
    st.session_state.backend_url = "https://course-compass-542057411868.us-east1.run.app/llm/predict"


# BASE_URL = "https://course-compass-542057411868.us-east1.run.app/"
# BACKEND_ENDPOINT = BASE_URL + "/llm/predict"

# Function to send a query to the backend
def send_query(session_id, query):
    response = requests.post(st.session_state.backend_url, json={"session_id": session_id, "query": query})
    return response.json()

# Initialize session state
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []

# UI Layout
st.title("📚 CourseCompass Assistant")
st.write("Your friendly assistant for course registration queries!")

with st.sidebar:
    reset_button = st.button("Reset Chat")
    if reset_button:
        st.session_state.session_id = None
        st.session_state.messages = []
        st.success("Chat restarted! Start a new conversation.")

# Input box for user query
user_input = st.chat_input("Enter your query")

# Handle new query
if user_input:
    # Generate a new session ID if it's the first message
    if st.session_state.session_id is None:
        st.session_state.session_id = str(uuid.uuid4())
    
    # Send the query to the backend
    with st.spinner("Generating response..."):
        response = send_query(st.session_state.session_id, user_input)

    # logging.info(f"Response from backend: {response}")
    # print(f"Response from backend: {response}")
    
    # Add user and assistant messages to the chat history
    st.session_state.messages.append({"role": "user", "content": user_input})
    st.session_state.messages.append({"role": "assistant", "content": response.get("response", "I'm sorry, I didn't understand that.")})

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])
