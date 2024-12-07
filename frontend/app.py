import logging
import streamlit as st
import uuid
import requests
from PIL import Image

logging.basicConfig(level=logging.INFO)

# Backend URL (replace with your backend endpoint)

if 'base_url' not in st.session_state:
    st.session_state.base_url = "https://course-compass-542057411868.us-east1.run.app/"

if 'backend_url' not in st.session_state:
    st.session_state.backend_url = "https://course-compass-542057411868.us-east1.run.app/llm/predict"


# BASE_URL = "https://course-compass-542057411868.us-east1.run.app/"
# BACKEND_ENDPOINT = BASE_URL + "/llm/predict"

# Function to send a query to the backend
def send_query(session_id, query):
    try:
        response = requests.post(st.session_state.backend_url, json={"session_id": session_id, "query": query})
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending query to backend: {e}")
        return {"error": "Error sending query to backend"}
    return response.json()

# Initialize session state
if "session_id" not in st.session_state:
    st.session_state.session_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []
    
# Inject HTML content into the SidebarHeader
custom_sidebar_header = """
    <style>
    [data-testid="stSidebarHeader"] {
        display: none;
    }
    </style>
"""

custom_sidebar_css = """
    <style>
        [data-testid="element-container"] > div:first-child {
            display: flex;
            justify-content: center;
            height: 100%;
        }
        [data-testid="stMarkdownContainer"] {
            display: flex;
            justify-content: center;
        }
        .stHeading {
            display: flex;
            justify-content: center;
        }
        .stImage {
            display: flex;
            justify-content: center;
        }
    </style>
"""

st.set_page_config(
    page_title="Course Compass",  # Title of the browser tab
    page_icon="nu_logo_transparent.png",  # Path to the favicon
    layout="wide",  # Optional: Sets the layout to wide mode
)

# UI Layout
st.title("📚 Course Compass Assistant")
st.write("Your friendly assistant for course registration queries!")

with st.sidebar:
    # st.markdown(custom_sidebar_header, unsafe_allow_html=True)
    st.markdown(custom_sidebar_css, unsafe_allow_html=True)
    st.image("nu_logo_transparent.png", width=120)
    st.header("Course Compass")
    reset_button = st.button("Reset Chat", key="reset_chat")
    st.markdown("Please reset the chat if you want to start a new conversation on a different course")
    if reset_button:
        st.session_state.session_id = None
        st.session_state.messages = []
        st.success("Chat restarted! Start a new conversation.")

# Input box for user query
user_input = st.chat_input("Enter your query")

# Handle new query
if user_input:
    # Generate a new session ID if it's the first message
    logging.info(f"User input: {user_input}, Session ID: {st.session_state.session_id}")
    if st.session_state.session_id is None:
        st.session_state.session_id = str(uuid.uuid4())
    
    # Send the query to the backend
    with st.spinner("Generating response..."):
        response = send_query(st.session_state.session_id, user_input)
        if "error" in response:
            st.error("Encountered an error while processing your query.")
            st.stop()

    # logging.info(f"Response from backend: {response}")
    # print(f"Response from backend: {response}")
    
    # Add user and assistant messages to the chat history
    st.session_state.messages.append({"role": "user", "content": user_input})
    st.session_state.messages.append({"role": "assistant", "content": response.get("response", "I'm sorry, I didn't understand that.")})

    # Display chat history
    for message in st.session_state.messages:
        if message["role"] == "user":
            with st.chat_message(message["role"], avatar="👨‍🎓"):
                st.write(message["content"])
        else:
            with st.chat_message(message["role"], avatar=Image.open('Northeastern_Huskies.png')):
                st.write(message["content"])
