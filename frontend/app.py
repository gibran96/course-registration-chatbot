import logging
import streamlit as st
import uuid
import requests

# Backend URL (replace with your backend endpoint)
BASE_URL = "https://course-compass-542057411868.us-east1.run.app/"
BACKEND_ENDPOINT = BASE_URL + "/llm/predict"

# Function to send a query to the backend
def send_query(session_id, query):
    response = requests.post(BACKEND_ENDPOINT, json={"session_id": session_id, "query": query})
    return response.json()

# Initialize session state
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []

# UI Layout
st.title("📚 CourseCompass Assistant")
st.write("Your friendly assistant for course registration queries!")

# Restart chat button
if st.button("Restart Chat"):
    st.session_state.session_id = None
    st.session_state.messages = []
    st.success("Chat restarted! Start a new conversation.")

# Input box for user query
user_input = st.text_input("Ask your question:")

# Handle new query
if user_input:
    # Generate a new session ID if it's the first message
    if st.session_state.session_id is None:
        st.session_state.session_id = str(uuid.uuid4())
    
    # Send the query to the backend
    response = send_query(st.session_state.session_id, user_input)

    # logging.info(f"Response from backend: {response}")
    print(f"Response from backend: {response}")
    
    # Add user and assistant messages to the chat history
    st.session_state.messages.append({"role": "user", "content": user_input})
    st.session_state.messages.append({"role": "assistant", "content": response.get("response:w", "I'm sorry, I didn't understand that.")})

# Display chat history
for message in st.session_state.messages:
    if message["role"] == "user":
        st.markdown(f"**You:** {message['content']}")
    elif message["role"] == "assistant":
        st.markdown(f"**CourseCompass:** {message['content']}")
