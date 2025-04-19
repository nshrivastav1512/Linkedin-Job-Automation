# simple_api_test.py
import google.generativeai as genai
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load API Key from .env file in the current directory
env_path = '.env'
api_key_name = 'GEMINI_API_KEY' # Make sure this matches your .env file

logging.info(f"Loading environment variables from: {env_path}")
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
    api_key = os.getenv(api_key_name)
    if not api_key:
        logging.error(f"{api_key_name} not found in {env_path}.")
        exit()
    logging.info(f"{api_key_name} loaded successfully.")
else:
    logging.error(f".env file not found at {env_path}. Cannot load API key.")
    exit()

# Configure Gemini
try:
    genai.configure(api_key=api_key)
    logging.info("Gemini API configured successfully.")
except Exception as e:
    logging.error(f"Failed to configure Gemini API: {e}")
    exit()

# Make a simple API call
try:
    model_name = 'gemini-2.0-flash' # Use the model you were using
    logging.info(f"Attempting simple API call to model: {model_name}")
    model = genai.GenerativeModel(model_name)
    # Use a very simple, short prompt
    response = model.generate_content("Explain what an API is in one sentence.")
    logging.info("API call successful.")
    logging.info(f"Response Text: {response.text}")
except Exception as e:
    logging.error(f"Error during simple API call: {e}", exc_info=True)

logging.info("Simple API test finished.")