# phase3_ai_analysis.py
# Phase 3: Uses AI (Gemini) to analyze job descriptions and compare them with the resume.
# Modification: Reads Resume.html, extracts text, performs analysis, adds detailed score columns, formats text outputs.

import os
import time
import traceback
import re
import json
import logging
import random # Needed for call_gemini_api retry delay
import pandas as pd
import numpy as np # Import numpy for checking array types if needed
from pathlib import Path # Use pathlib for consistency
from dotenv import load_dotenv
from bs4 import BeautifulSoup # Needed for HTML text extraction and tag stripping

# Attempt to import AI library
try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
except ImportError:
    logging.critical("CRITICAL ERROR: google-generativeai library not installed. Run 'pip install google-generativeai'")
    raise # Stop script if essential library is missing

# --- Column Definitions ---
# **IMPORTANT**: Make sure this list is updated in phase1_list_scraper.py as well!
ALL_EXPECTED_COLUMNS = [
    'Job ID', 'Title', 'Company', 'Location', 'Workplace Type', 'Link', 'Easy Apply', 'Promoted', 'Viewed',
    'Early Applicant', 'Verified', 'Posted Ago Text', 'Posted Days Ago', 'Posted Hours Ago', 'Salary Range',
    'Insights', 'Company Logo URL', 'Source', 'Date Added', 'Status', 'Applied Date', 'Notes',
    'Applicant Count', 'Job Description HTML', 'Job Description Plain Text', 'About Company',
    'Date Scraped Detailed', 'Posted Ago Text Detailed', 'Company LinkedIn URL', 'Company Industry',
    'Company Size', 'Company LinkedIn Members', 'Company Followers', 'Hiring Team Member 1 Name',
    'Hiring Team Member 1 Profile URL', 'Hiring Team Member 2 Name', 'Hiring Team Member 2 Profile URL',
    'Skills Required', # Kept for compatibility, maybe populated by phase 2?
    # Phase 3 - Text format outputs
    'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills',
    'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description',
    # Phase 3 - AI Analysis Outputs
    'AI Match Score', # Original overall score (0-5)
    'AI Score Justification', # Combined text justification
    'AI Strengths', # Bulleted text
    'AI Areas for Improvement', # Bulleted text
    'AI Actionable Recommendations', # Bulleted text + Evaluation Breakdown
    # Phase 3 - NEW DETAILED SCORE COLUMNS
    'Keyword Match Score',      # Numerical score (e.g., 0.0 to 1.0)
    'Achievements Score',       # Numerical score
    'Summary Quality Score',    # Numerical score
    'Structure Score',          # Numerical score
    'Tools Certs Score',        # Numerical score
    'Total Match Score',        # Calculated sum for threshold check
    # Phase 4 Tailoring Output
    'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List',
    'Tailored HTML Path', 'Tailored PDF Path'
]
# Columns specifically added/updated by THIS phase
PHASE_3_OUTPUT_COLUMNS = [
    'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills',
    'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description',
    'AI Match Score', 'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement',
    'AI Actionable Recommendations', 'Keyword Match Score', 'Achievements Score',
    'Summary Quality Score', 'Structure Score', 'Tools Certs Score', 'Total Match Score'
]

# --- Helper Functions ---

def strip_html_tags(html_text: str) -> str:
    """Removes HTML tags from a string, used for saving clean text."""
    if not html_text or not isinstance(html_text, str):
        return ""
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        return soup.get_text(separator=' ', strip=True) # Use space separator
    except Exception:
        # Fallback: try basic regex for simple tags if BS4 fails (less robust)
        try: return re.sub(r'<[^>]+>', ' ', html_text).strip()
        except: return html_text # Return original on error

# --- REVISED format_list_as_bullets function ---
def format_list_as_bullets(data_input: list | str | pd.Series | np.ndarray | None, indent="- ") -> str:
    """Formats a list (or JSON string list, Series, array) into a bulleted plain text string."""

    # 1. Handle None input FIRST
    if data_input is None:
        return "N/A"

    items_to_format = [] # Initialize empty list for items

    # 2. Handle list or tuple input
    if isinstance(data_input, (list, tuple)):
        items_to_format = list(data_input)
    # 3. Handle pandas Series or NumPy array input
    elif isinstance(data_input, (pd.Series, np.ndarray)):
        # Convert array/Series to list, explicitly dropping NA/None values during conversion
        items_to_format = [item for item in data_input if pd.notna(item) and item is not None]
    # 4. Handle string input (could be JSON list or plain string)
    elif isinstance(data_input, str):
        try:
            parsed_list = json.loads(data_input)
            if isinstance(parsed_list, list):
                items_to_format = parsed_list # Successfully parsed list
            else: # Parsed JSON wasn't a list
                items_to_format = [str(parsed_list)] # Treat as single item
        except json.JSONDecodeError:
            # If not JSON or parsing failed, treat as single string item
            items_to_format = [data_input] if data_input.strip() else []
    # 5. Handle SCALAR input (after checking for list/array/str)
    else:
        # Now it's safe to use pd.isna for scalar check
        if pd.isna(data_input):
            return "N/A"
        else: # Treat as single item list
            items_to_format = [str(data_input)]

    # --- Process the items_to_format list ---
    if not items_to_format: # Check if list is empty after processing
        return "N/A"

    # Clean items (remove potential HTML, convert to string, strip whitespace)
    cleaned_items = [strip_html_tags(str(item)).strip() for item in items_to_format]
    # Filter out empty items after cleaning
    valid_items = [item for item in cleaned_items if item]

    if not valid_items:
        return "N/A"

    return "\n".join([f"{indent}{item}" for item in valid_items])
# --- END REVISED format_list_as_bullets function ---


def load_api_key_and_resume_html(config: dict) -> tuple[str | None, str | None]:
    """Loads API key from .env and reads the HTML resume content."""
    api_key = None
    resume_html_content = None
    env_path = config['paths']['env_filepath']
    api_key_name = config['ai']['api_key_name']
    resume_html_path = config['ai']['resume_html_filepath']

    # Load API Key
    logging.info(f"Loading environment variables from: {env_path}")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        api_key = os.getenv(api_key_name)
        if not api_key: logging.error(f"{api_key_name} not found in {env_path}.")
        else: logging.info(f"{api_key_name} loaded successfully.")
    else: logging.error(f".env file not found at {env_path}. Cannot load API key.")

    # Load Resume HTML Content
    logging.info(f"Reading resume HTML file from: {resume_html_path}")
    try:
        if not isinstance(resume_html_path, Path): resume_html_path = Path(resume_html_path)
        if not resume_html_path.is_file():
             raise FileNotFoundError(f"Resume HTML file not found at '{resume_html_path}'.")
        with open(resume_html_path, 'r', encoding='utf-8') as f:
            resume_html_content = f.read()
        if not resume_html_content:
             logging.error(f"Resume HTML file '{resume_html_path}' is empty.")
             resume_html_content = None
        else:
             logging.info(f"Loaded resume HTML content (length: {len(resume_html_content)} characters).")
    except FileNotFoundError as e: logging.error(e); resume_html_content = None
    except Exception as e: logging.error(f"Failed to read resume HTML: {e}", exc_info=True); resume_html_content = None

    return api_key, resume_html_content

def extract_text_from_html(html_content: str) -> str | None:
    """Extracts plain text from HTML resume content."""
    if not html_content: logging.error("Cannot extract text from empty HTML."); return None
    logging.debug("Extracting plain text from HTML resume content...")
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        container = soup.find('div', class_='container') or soup.body
        if not container:
             logging.warning("Could not find 'div.container' or 'body' tag in HTML for text extraction.")
             container = soup # Fallback

        for script_or_style in container(["script", "style"]): script_or_style.decompose()
        text = container.get_text(separator='\n', strip=True)
        text = re.sub(r'\n{3,}', '\n\n', text) # Consolidate blank lines

        if len(text) < 100: logging.warning(f"Extracted text seems very short ({len(text)} chars).")
        logging.debug(f"Extracted plain text length: {len(text)}")
        return text
    except Exception as e: logging.error(f"Error extracting text from HTML: {e}", exc_info=True); return None


def configure_gemini(api_key: str, config: dict) -> bool:
    """Configures the Google Gemini API client."""
    if not api_key: logging.error("Cannot configure Gemini: API key missing."); return False
    try:
        genai.configure(api_key=api_key)
        logging.info("Gemini API configured successfully.")
        return True
    except Exception as e: logging.error(f"Failed to configure Gemini API: {e}", exc_info=True); return False

def parse_safety_settings(config: dict) -> dict | None:
    """Parses safety settings from config string values to HarmBlockThreshold enums."""
    settings_dict = config['ai'].get('safety_settings', {})
    parsed_settings = {}
    try:
        for key_str, value_str in settings_dict.items():
            category_enum = getattr(HarmCategory, key_str, None)
            threshold_enum = getattr(HarmBlockThreshold, value_str, None)
            if category_enum and threshold_enum: parsed_settings[category_enum] = threshold_enum
            else: logging.warning(f"Invalid safety setting: {key_str}={value_str}. Skipping.")
        return parsed_settings
    except Exception as e: logging.error(f"Error parsing safety settings: {e}", exc_info=True); return None

def call_gemini_api(model_name: str, prompt_text: str, config: dict, is_json_output: bool, attempt=1, max_attempts=3) -> dict | str:
    """Makes a call to the Gemini API with error handling, retries, and configuration."""
    api_delay = config['ai']['api_delay_seconds']
    gen_config_key = 'generation_config_json' if is_json_output else 'generation_config_text'
    generation_config = config['ai'].get(gen_config_key, {})
    safety_settings = parse_safety_settings(config)

    logging.info(f"Calling Gemini model '{model_name}' (Attempt {attempt}/{max_attempts}). Expecting {'JSON' if is_json_output else 'Text'}.")

    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(
            prompt_text,
            generation_config=generation_config,
            safety_settings=safety_settings
        )

        # --- Safety/Block Checks ---
        if response.prompt_feedback and response.prompt_feedback.block_reason:
            reason_name = response.prompt_feedback.block_reason.name
            logging.error(f"Gemini Prompt BLOCKED due to safety settings. Reason: {reason_name}")
            error_msg = f"ERROR: Prompt blocked by safety settings ({reason_name})"
            return {"error": error_msg} if is_json_output else error_msg

        if not response.candidates:
             finish_reason_detail = "N/A"
             try:
                 if hasattr(response, 'candidates') and response.candidates and hasattr(response.candidates[0], 'finish_reason'):
                    finish_reason_detail = response.candidates[0].finish_reason.name
             except Exception: pass
             logging.error(f"Gemini response has no candidates. Finish Reason: {finish_reason_detail}")
             error_msg = f"ERROR: No response candidates (Finish Reason: {finish_reason_detail})"
             return {"error": error_msg} if is_json_output else error_msg

        candidate = response.candidates[0]
        candidate_finish_reason = getattr(candidate, 'finish_reason', None)
        finish_reason_name = getattr(candidate_finish_reason, 'name', 'UNKNOWN')

        if finish_reason_name != 'STOP':
            logging.warning(f"Gemini response candidate finished with reason: {finish_reason_name}")
            if finish_reason_name == 'MAX_TOKENS':
                error_msg = "ERROR: Output truncated due to MAX_TOKENS limit."
                try: partial_text = response.text
                except (ValueError, AttributeError): partial_text = None
                if is_json_output: return {"error": error_msg, "partial_response": partial_text}
                else: return error_msg + (f"\nPartial Text:\n{partial_text}" if partial_text else "")
            else: # Safety, Recitation, Other, Unknown
                 error_msg = f"ERROR: Response stopped unexpectedly (Reason: {finish_reason_name})"
                 return {"error": error_msg} if is_json_output else error_msg

        # --- Access Text Content ---
        try:
            response_text = response.text
            logging.info("Gemini API call successful.")
            if is_json_output:
                try:
                    # Handle potential markdown fences
                    cleaned_text = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE | re.DOTALL).strip()
                    if not cleaned_text: return {"error": "Empty JSON response after cleaning"}
                    parsed_json = json.loads(cleaned_text)
                    logging.debug("Successfully parsed JSON response.")
                    return parsed_json
                except json.JSONDecodeError as json_err:
                    logging.error(f"Failed JSON decode: {json_err}. Raw:\n{response_text[:500]}...")
                    return {"error": "Failed to parse JSON response", "raw_response": response_text}
                except Exception as parse_e:
                    logging.error(f"Unexpected JSON parse error: {parse_e}", exc_info=True)
                    return {"error": f"Unexpected parsing error: {parse_e}", "raw_response": response_text}
            else:
                return response_text
        except (ValueError, AttributeError) as ve:
             final_finish_reason_name = getattr(getattr(candidate, 'finish_reason', None), 'name', 'UNKNOWN')
             logging.error(f"Gemini response generated no valid text content. Finish Reason: {final_finish_reason_name}. Error: {ve}")
             safety_block_reason = None
             try:
                 if candidate.safety_ratings:
                      for rating in candidate.safety_ratings:
                          if rating.block: safety_block_reason = rating.category.name; break
             except Exception: pass

             if safety_block_reason: error_msg = f"ERROR: Content blocked by safety filter ({safety_block_reason})"
             else: error_msg = f"ERROR: Response has no text content (Finish Reason: {final_finish_reason_name})"
             return {"error": error_msg} if is_json_output else error_msg

    except Exception as e:
        logging.error(f"Error during Gemini API call/request: {type(e).__name__} - {e}", exc_info=True)
        if ("Resource has been exhausted" in str(e) or "429" in str(e) or "503" in str(e)):
            if attempt < max_attempts:
                wait_time = api_delay * (1.5 ** attempt) + random.uniform(0, 1)
                logging.warning(f"API busy/rate limit. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                return call_gemini_api(model_name, prompt_text, config, is_json_output, attempt + 1, max_attempts)
            else: error_msg = "ERROR: API rate limit/busy after multiple retries"
        elif "API key not valid" in str(e): error_msg = "ERROR: Invalid Gemini API Key."
        else: error_msg = f"ERROR: API Call Failed - {type(e).__name__}"
        return {"error": error_msg} if is_json_output else error_msg

# --- Gemini Call 1: Extraction ---
def extract_job_details_with_gemini(jd_plain_text: str, config: dict) -> dict:
    """Uses Gemini to extract structured info (as JSON) from the job description."""
    if not jd_plain_text or pd.isna(jd_plain_text) or len(jd_plain_text) < 50:
        logging.warning("JD text too short/invalid for AI extraction.")
        return {"error": "Invalid job description text"}
    logging.info("Preparing prompt for Job Detail Extraction (AI Call 1 - Expecting JSON)...")
    prompt = f"""
    Analyze the following job description text and extract the requested information.
    Format the output strictly as a single JSON object. Do NOT include any introductory text, explanations, or markdown formatting like ```json.
    Required JSON Keys:
    - "Key Responsibilities": [List of strings summarizing main duties, or a single concise string].
    - "Required Skills": [List of specific technical and soft skills explicitly mentioned as REQUIRED].
    - "Preferred Skills": [List of skills mentioned as PREFERRED, "nice-to-have", or advantageous. Use [] if none].
    - "Required Experience Level": [String summarizing years/level (e.g., "3-5 years", "Senior", "Entry-level"). Use "Not Specified" if not found].
    - "Key Qualifications": [List of specific degrees, certifications, or crucial non-skill qualifications. Use [] if none].
    - "Concise Company Description": [1-2 sentence summary of the company IF described within this text. Use "Not Specified" if not found].

    Job Description Text:
    ---
    {jd_plain_text[:8000]}
    ---
    Output ONLY the JSON object.
    """
    model_name = config['ai']['extraction_model_name']
    response_data = call_gemini_api(model_name, prompt, config, is_json_output=True)

    # --- Return raw dict/error dict ---
    if isinstance(response_data, dict) and "error" in response_data:
        logging.error(f"AI extraction failed: {response_data['error']}")
        return response_data # Return error dict
    if not isinstance(response_data, dict):
        logging.error(f"AI extraction returned unexpected type: {type(response_data)}. Expected dict.")
        return {"error": "Invalid response format from API", "raw_response": str(response_data)}

    # Optional: Basic check for expected keys (logging only)
    expected_keys = ["Key Responsibilities", "Required Skills", "Preferred Skills", "Required Experience Level", "Key Qualifications", "Concise Company Description"]
    missing_keys = [key for key in expected_keys if key not in response_data]
    if missing_keys:
        logging.warning(f"AI extraction response missing keys: {missing_keys}")
        # Don't add keys here, let the processing function handle defaults

    logging.info("AI extraction successful (returned JSON dictionary).")
    return response_data # Return the successful JSON dictionary

# --- Gemini Call 2: Analysis & Scoring ---
def analyze_resume_fit_with_gemini(resume_plain_text: str, jd_plain_text: str, config: dict) -> dict:
    """
    Uses Gemini for analysis, expects detailed breakdown in response text.
    Returns a dictionary containing parsed elements and scores.
    """
    if not resume_plain_text or len(resume_plain_text) < 100:
        logging.error("Resume text missing/too short for analysis.")
        return {"error": "Invalid resume text"}
    if not jd_plain_text or len(jd_plain_text) < 50:
        logging.warning("JD text too short/invalid for AI analysis.")
        return {"error": "Invalid job description text"}

    logging.info("Preparing prompt for Resume Fit Analysis (AI Call 2 - Expecting Text with Breakdown)...")
    prompt = f"""
    **Task:** Evaluate the provided resume against the given job description to assess alignment based on specific criteria and provide a 5-star rating and detailed feedback including a scoring breakdown.

    **Inputs:**
    *   **Resume Text:**
        ```
        {resume_plain_text[:8000]}
        ```
    *   **Job Description Text:**
        ```
        {jd_plain_text[:8000]}
        ```

    **Evaluation Criteria & Scoring (Total 5 Stars Possible):**
    1.  **Keyword and Skill Match (Max 1 Star)** Score: 1.0★ (90–100% match), 0.75★ (75–89%), 0.5★ (50–74%), 0.25★ (<50%)
    2.  **Quantifiable Achievements (Max 1 Star)** Score: 1.0★ (5+ results), 0.75★ (3–4 results), 0.5★ (1–2 results), 0.25★ (0 results)
    3.  **Professional Summary and Content Quality (Max 1 Star)** Score: 1.0★ (Tailored, impactful, concise), 0.75★ (Mostly tailored), 0.5★ (Generic), 0.25★ (Poor)
    4.  **Resume Structure and Formatting (Max 1 Star)** Score: 1.0★ (Well-structured, clear, ATS-friendly), 0.75★ (Minor issues), 0.5★ (Several issues), 0.25★ (Poor)
    5.  **Relevant Tools and Certifications (Max 1 Star)** Score: 1.0★ (100% relevant mentioned), 0.75★ (75–99%), 0.5★ (50–74%), 0.25★ (<50%)

    **Final Rating Calculation:** Sum scores (max 5.0).
    **Star Rating Scale:** 5★: 4.75–5.00 (Exceptional), 4★: 3.75–4.74 (Strong), 3★: 2.75–3.74 (Moderate), 2★: 1.75–2.74 (Below average), 1★: 0.00–1.74 (Poor fit)

    **Expected Output Format:** Start EXACTLY with "Overall Star Rating:". Follow with Strengths, Areas for Improvement, Actionable Recommendations. **MANDATORY: Conclude with the full "Evaluation Breakdown:" section. Each of the 5 numbered lines in the breakdown MUST include the score (e.g., 0.75★) followed by a concise justification or reason for that specific score.** No markdown formatting.

    **Output Structure Example:**
    Overall Star Rating: [Score] out of 5 Stars ([Category])

    Strengths:
    - [Highlight 1]
    - [...]

    Areas for Improvement:
    - [Suggestion 1]
    - [...]

    Actionable Recommendations:
    - [Action 1]
    - [...]

    Evaluation Breakdown:
    1. Keyword and Skill Match: [Score]★ - [Justification text for score 1]
    2. Quantifiable Achievements: [Score]★ - [Justification text for score 2]
    3. Professional Summary and Content Quality: [Score]★ - [Justification text for score 3]
    4. Resume Structure and Formatting: [Score]★ - [Justification text for score 4]
    5. Relevant Tools and Certifications: [Score]★ - [Justification text for score 5]

    **Generate the analysis text now, ensuring the detailed justification is present for each breakdown item.**
    """
    model_name = config['ai']['analysis_model_name']
    response_text = call_gemini_api(model_name, prompt, config, is_json_output=False)

    # Initialize results dictionary with defaults for all expected fields
    analysis_results = {
        "AI Match Score": pd.NA, "Rating Category": "N/A",
        "AI Strengths": "N/A", "AI Areas for Improvement": "N/A",
        "AI Actionable Recommendations": "N/A", # This will include the breakdown
        "Keyword Match Score": pd.NA, "Achievements Score": pd.NA,
        "Summary Quality Score": pd.NA, "Structure Score": pd.NA,
        "Tools Certs Score": pd.NA,
        "_full_response": response_text,
        "_parse_successful": False,
        "error": None
    }

    if isinstance(response_text, dict) and "error" in response_text: # Handle API error dict
        logging.error(f"AI analysis API call failed: {response_text['error']}")
        analysis_results["error"] = response_text['error']
        analysis_results["AI Actionable Recommendations"] = response_text['error'] # Put error here for visibility
        return analysis_results
    elif not isinstance(response_text, str) or response_text.startswith("ERROR:"): # Handle simple error string
        err_msg = response_text if isinstance(response_text, str) else "Unknown API Error"
        logging.error(f"AI analysis API call failed: {err_msg}")
        analysis_results["error"] = err_msg
        analysis_results["AI Actionable Recommendations"] = err_msg
        return analysis_results

    try:
        logging.info("Parsing AI analysis response...")
        analysis_results["_full_response"] = response_text # Store raw response

        # --- Parse Overall Rating ---
        rating_match = re.search(r"Overall Star Rating:\s*([\d\.]+)\s*out of 5 Stars?\s*\((.*?)\)", response_text, re.IGNORECASE | re.MULTILINE)
        if rating_match:
            try: analysis_results["AI Match Score"] = float(rating_match.group(1).strip())
            except ValueError: analysis_results["AI Match Score"] = pd.NA; logging.warning("Could not parse overall score as float.")
            analysis_results["Rating Category"] = rating_match.group(2).strip()
            logging.debug(f"  Parsed Overall Rating: {analysis_results['AI Match Score']} ({analysis_results['Rating Category']})")
        else:
            logging.warning("  Could not parse 'Overall Star Rating' line.")
            analysis_results["AI Match Score"] = pd.NA

        # --- Parse Text Sections ---
        # Use raw f-strings (rf"...") for regex patterns to avoid SyntaxWarning
        def extract_section(header, text, include_breakdown=False):
            start_match = re.search(rf"^{re.escape(header)}:?\s*$", text, re.IGNORECASE | re.MULTILINE)
            if not start_match: return None
            start_index = start_match.end()

            end_index = len(text)
            next_headers = ["Strengths:", "Areas for Improvement:", "Actionable Recommendations:", "Evaluation Breakdown:"]
            for next_header in next_headers:
                if next_header.lower() != header.lower():
                     next_match = re.search(rf"^{re.escape(next_header)}:?\s*$", text[start_index:], re.IGNORECASE | re.MULTILINE)
                     if next_match:
                         end_index = min(end_index, start_index + next_match.start())

            section_text = text[start_index:end_index].strip()
            if header.lower() != "actionable recommendations:":
                section_text = re.sub(r'^\s*[-*]\s*', '', section_text, flags=re.MULTILINE).strip()

            if header.lower() == "actionable recommendations:" and include_breakdown:
                 breakdown_match = re.search(r"Evaluation Breakdown:", section_text, re.IGNORECASE | re.MULTILINE)
                 if not breakdown_match:
                     full_breakdown_match = re.search(r"Evaluation Breakdown:(.*)", text, re.IGNORECASE | re.DOTALL)
                     if full_breakdown_match:
                         section_text += "\n\nEvaluation Breakdown:" + full_breakdown_match.group(1).strip()
                         logging.debug("  Appended missing Evaluation Breakdown to Recommendations.")

            return section_text if section_text else None

        analysis_results["AI Strengths"] = extract_section("Strengths", response_text) or "Parsing Error"
        analysis_results["AI Areas for Improvement"] = extract_section("Areas for Improvement", response_text) or "Parsing Error"
        analysis_results["AI Actionable Recommendations"] = extract_section("Actionable Recommendations", response_text, include_breakdown=True) or "Parsing Error"


# --- Parse Evaluation Breakdown Scores and Justifications ---
        logging.debug("  Parsing Evaluation Breakdown scores and justifications...")
        breakdown_details = { # Dictionary to store the full formatted lines
            "Keyword Match": "1. Keyword and Skill Match: N/A - Parsing Error",
            "Achievements": "2. Quantifiable Achievements: N/A - Parsing Error",
            "Summary Quality": "3. Professional Summary and Content Quality: N/A - Parsing Error",
            "Structure": "4. Resume Structure and Formatting: N/A - Parsing Error",
            "Tools Certs": "5. Relevant Tools and Certifications: N/A - Parsing Error",
        }
        # Regex to capture: 1) score, 2) everything else on the line
        score_pattern_template = r"^{num}\.\s+.*?:\s*([\d\.]+)★?(.*)$"

        score_column_keys = {
            1: "Keyword Match Score",
            2: "Achievements Score",
            3: "Summary Quality Score",
            4: "Structure Score",
            5: "Tools Certs Score",
        }
        # Map number to the key used in breakdown_details dict
        detail_key_map = {
            1: "Keyword Match",
            2: "Achievements",
            3: "Summary Quality",
            4: "Structure",
            5: "Tools Certs",
        }

        # Search within the full response text to find the breakdown reliably
        breakdown_section_match = re.search(r"Evaluation Breakdown:(.*)", response_text, re.IGNORECASE | re.DOTALL)
        breakdown_search_area = breakdown_section_match.group(1) if breakdown_section_match else ""

        if not breakdown_search_area:
            logging.error("Could not find the 'Evaluation Breakdown:' section header in the response.")
            # Keep scores NA and details as parsing error
        else:
            logging.debug("Found 'Evaluation Breakdown:' section. Parsing lines...")
            lines = breakdown_search_area.strip().split('\n')
            found_any_breakdown = False

            for line in lines:
                line = line.strip()
                if not line: continue

                # Check which number the line starts with
                line_num_match = re.match(r"^([1-5])\.", line)
                if not line_num_match: continue # Skip lines not starting with 1-5.

                num = int(line_num_match.group(1))
                pattern = score_pattern_template.format(num=num)
                match = re.search(pattern, line, re.IGNORECASE) # Search only on the specific line

                score_col_key = score_column_keys[num]
                detail_key = detail_key_map[num]

                if match:
                    found_any_breakdown = True
                    # Parse numerical score
                    try:
                        score_float = float(match.group(1).strip())
                        analysis_results[score_col_key] = score_float
                        logging.debug(f"    Parsed {score_col_key}: {score_float}")
                    except ValueError:
                        logging.warning(f"    Could not parse float for {score_col_key} from '{match.group(1)}'")
                        analysis_results[score_col_key] = pd.NA

                    # Capture the justification text following the score
                    justification_text = match.group(2).strip()
                    # Store the *original matched line* (or reconstruct carefully)
                    # Using the original line is safest if the pattern matched
                    breakdown_details[detail_key] = line
                    logging.debug(f"    Captured Detail Line for {detail_key}: {line}")

                else:
                     logging.warning(f"    Pattern did not match expected format for line starting with {num}: '{line}'")
                     # Keep score NA (already default)
                     breakdown_details[detail_key] = f"{num}. {detail_key.replace(' ', '')}: N/A - Line Format Error" # Update detail error

            if not found_any_breakdown:
                 logging.error("Found 'Evaluation Breakdown' header but failed to parse any numbered criteria lines.")

        # --- Construct Detailed Evaluation Breakdown Text ---
        # Use the captured/default lines from breakdown_details
        evaluation_breakdown_text = f"""
Evaluation Breakdown:
{breakdown_details["Keyword Match"]}
{breakdown_details["Achievements"]}
{breakdown_details["Summary Quality"]}
{breakdown_details["Structure"]}
{breakdown_details["Tools Certs"]}
""".strip()

        # --- Finalize AI Actionable Recommendations ---
        # Always use the reconstructed/captured breakdown text
        recommendations_text_only = analysis_results.get("AI Actionable Recommendations", "Parsing Error")
        # Find the end of the actionable recommendations part to insert breakdown after
        rec_end_match = re.search(r"Actionable Recommendations:(.*?)(\n\s*Evaluation Breakdown:|$)", recommendations_text_only, re.IGNORECASE | re.DOTALL)
        if rec_end_match:
             # Get text before breakdown, strip it, then add the detailed breakdown
             recommendations_core_text = rec_end_match.group(1).strip()
             analysis_results["AI Actionable Recommendations"] = f"{recommendations_core_text}\n\n{evaluation_breakdown_text}"
        else: # Fallback: if structure weird, just append
             analysis_results["AI Actionable Recommendations"] = f"{recommendations_text_only}\n\n{evaluation_breakdown_text}".strip()
        logging.debug("Ensured detailed Evaluation Breakdown is in Recommendations text.")
        # --- Final Justification ---
        analysis_results["AI Score Justification"] = f"Score: {analysis_results['AI Match Score']} ({analysis_results['Rating Category']})\n\nStrengths:\n{analysis_results['AI Strengths']}\n\nAreas for Improvement:\n{analysis_results['AI Areas for Improvement']}\n\nActionable Recommendations:\n{analysis_results['AI Actionable Recommendations']}".strip()

        analysis_results["_parse_successful"] = True
        logging.info("Successfully parsed AI analysis response.")

    except Exception as e:
        logging.error(f"Error parsing analysis text response: {e}", exc_info=True)
        analysis_results["error"] = f"Parse Error: {e}"
        analysis_results["AI Actionable Recommendations"] = f"ERROR PARSING RESPONSE. Raw Text:\n{response_text[:1000]}..." # Update recommendations with error

    return analysis_results


# --- Main Processing Function for Phase 3 ---
def process_ai_analysis(config: dict, resume_plain_text: str):
    """Reads Excel, runs AI analysis, updates DataFrame with formatted text & scores, saves."""
    # --- Config Extraction ---
    excel_filepath = config['paths']['excel_filepath']
    status_ready = config['status']['READY_FOR_AI']
    status_processing = config['status']['PROCESSING_AI']
    status_analyzed = config['status']['AI_ANALYZED']
    status_failed_extract = config['status']['FAILED_AI_EXTRACTION']
    status_failed_analyze = config['status']['FAILED_AI_ANALYSIS']
    status_missing_data = config['status']['MISSING_DATA']
    phase3_error_statuses = [status_failed_extract, status_failed_analyze, config['status'].get('FAILED_API_CONFIG'), status_missing_data]
    phase3_error_statuses = [s for s in phase3_error_statuses if s]

    save_interval = config['ai'].get('save_interval', 5)
    retry_failed = config['workflow']['retry_failed_phase3']

    logging.info(f"Starting AI analysis processing for: {excel_filepath}")
    logging.info(f"Retry previously failed rows: {retry_failed}")

    if not resume_plain_text:
        logging.error("Resume plain text is missing. Phase 3 cannot proceed.")
        return False

    try:
        logging.info("Reading Excel file...")
        df = pd.read_excel(excel_filepath, engine='openpyxl', dtype={'Job ID': str})
        # df = df.fillna('') # Fill NA after potential type conversions
        logging.info(f"Read {len(df)} rows.")

        # --- Schema Check & Update ---
        added_cols = False
        # Use the globally defined ALL_EXPECTED_COLUMNS which includes new score cols
        for col in ALL_EXPECTED_COLUMNS:
             if col not in df.columns:
                  logging.warning(f"Adding missing column '{col}' to DataFrame.")
                  # Initialize based on likely type
                  if 'Score' in col: df[col] = pd.NA # Use NA for numeric scores
                  else: df[col] = '' # Default to empty string
                  added_cols = True
        if added_cols:
             logging.info("Reordering DataFrame columns.")
             df = df.reindex(columns=ALL_EXPECTED_COLUMNS) # Reindex fills with NaN/NA by default

        # --- Data Type Conversion and Fill NA ---
        # Convert score columns to numeric, coercing errors to NaN (which becomes NA)
        score_cols = ['AI Match Score', 'Keyword Match Score', 'Achievements Score',
                      'Summary Quality Score', 'Structure Score', 'Tools Certs Score', 'Total Match Score']
        for col in score_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Ensure Status and JD Text are usable strings, filling NA with appropriate defaults
        df['Status'] = df['Status'].astype(str).replace('nan', '').fillna(config['status'].get("NEW", "New"))
        df['Job Description Plain Text'] = df['Job Description Plain Text'].astype(str).replace('nan', '').fillna('')
        # Fill NA in other potential text columns that might be used later
        for col in ['Title', 'Company', 'AI Strengths', 'AI Areas for Improvement', 'AI Actionable Recommendations']:
             if col in df.columns:
                  # Use fillna('N/A') or fillna('') depending on preference
                  df[col] = df[col].fillna('N/A').astype(str).replace('nan', 'N/A')


        # --- Filter rows for AI processing ---
        statuses_to_process = [status_ready]
        if retry_failed:
            statuses_to_process.extend(phase3_error_statuses)
            statuses_to_process = list(set(statuses_to_process))
            logging.info(f"Will process jobs with status in: {statuses_to_process}")
        else:
            logging.info(f"Will process jobs with status: ['{status_ready}']")

        rows_to_process_mask = df['Status'].isin(statuses_to_process)
        rows_to_process_idx = df[rows_to_process_mask].index
        num_to_process = len(rows_to_process_idx)
        logging.info(f"Found {num_to_process} rows matching processing criteria.")

        if num_to_process == 0:
            logging.info("No rows found needing AI analysis.")
            if added_cols: # Save if schema changed
                try:
                    logging.info("Saving Excel file due to schema changes.")
                    df_final_save = df.fillna('') # Fill NA before save for cleaner Excel
                    df_final_save.to_excel(excel_filepath, index=False, engine='openpyxl')
                except Exception as save_err: logging.error(f"Error saving schema changes: {save_err}"); return False
            return True

        # --- Processing Loop ---
        update_count = 0
        processed_in_run = 0
        batch_start_time = time.time()

        for index in rows_to_process_idx:
            processed_in_run += 1
            # Use .get() for safety in case columns were missing despite reindex
            job_title = df.get('Title', pd.Series(dtype=str)).loc[index]
            company_name = df.get('Company', pd.Series(dtype=str)).loc[index]
            jd_text = str(df.get('Job Description Plain Text', pd.Series(dtype=str)).loc[index]).strip()

            logging.info(f"--- Processing Row {index + 1}/{len(df)} (Index: {index}) | Job: '{job_title}' @ '{company_name}' ---")
            df.loc[index, 'Status'] = status_processing # Set status

            if not jd_text or len(jd_text) < 50:
                logging.warning(f"Skipping row {index + 1}: Invalid/missing 'Job Description Plain Text'.")
                df.loc[index, 'Status'] = status_missing_data
                continue

            analysis_step_successful = True
            api_error_message = ""

            # --- API Call 1: Information Extraction ---
            logging.info("  Starting Information Extraction...")
            extracted_info_dict = extract_job_details_with_gemini(jd_text, config)
            time.sleep(config['ai']['api_delay_seconds']) # Delay after call

            if "error" in extracted_info_dict:
                logging.error(f"  Extraction Failed: {extracted_info_dict['error']}")
                df.loc[index, 'Status'] = status_failed_extract
                api_error_message = extracted_info_dict['error']
                analysis_step_successful = False
                # Store raw response in Notes if available
                if 'raw_response' in extracted_info_dict and 'Notes' in df.columns:
                    df.loc[index, 'Notes'] = f"Phase 3 Extract Error: {extracted_info_dict.get('error', 'Unknown')} Raw:\n{str(extracted_info_dict.get('raw_response',''))[:500]}"
                # Set text fields to error indicator
                df.loc[index, 'Extracted Responsibilities'] = "Extraction Error"
                df.loc[index, 'Extracted Required Skills'] = "Extraction Error"
                df.loc[index, 'Extracted Preferred Skills'] = "Extraction Error"
                df.loc[index, 'Extracted Experience Level'] = "Extraction Error"
                df.loc[index, 'Extracted Key Qualifications'] = "Extraction Error"
                df.loc[index, 'Extracted Company Description'] = "Extraction Error"

            else:
                # Convert lists to bulleted text BEFORE saving to DataFrame
                # Pass the actual value retrieved from the dictionary
                df.loc[index, 'Extracted Responsibilities'] = format_list_as_bullets(extracted_info_dict.get("Key Responsibilities"))
                df.loc[index, 'Extracted Required Skills'] = format_list_as_bullets(extracted_info_dict.get("Required Skills"))
                df.loc[index, 'Extracted Preferred Skills'] = format_list_as_bullets(extracted_info_dict.get("Preferred Skills"))
                df.loc[index, 'Extracted Key Qualifications'] = format_list_as_bullets(extracted_info_dict.get("Key Qualifications"))
                # These are likely strings already, but handle N/A potential
                df.loc[index, 'Extracted Experience Level'] = str(extracted_info_dict.get("Required Experience Level", "N/A"))
                df.loc[index, 'Extracted Company Description'] = str(extracted_info_dict.get("Concise Company Description", "N/A"))
                logging.info("  Successfully stored extracted information (formatted as text).")

            # --- API Call 2: Resume Fit Analysis ---
            if analysis_step_successful:
                logging.info("  Starting Resume Fit Analysis...")
                analysis_results = analyze_resume_fit_with_gemini(resume_plain_text, jd_text, config)
                time.sleep(config['ai']['api_delay_seconds']) # Delay after call

                if analysis_results.get("error"):
                    logging.error(f"  Analysis Failed: {analysis_results['error']}")
                    df.loc[index, 'Status'] = status_failed_analyze
                    api_error_message = analysis_results['error']
                    analysis_step_successful = False
                    # Store error in actionable recommendations / notes
                    df.loc[index, 'AI Actionable Recommendations'] = analysis_results.get("AI Actionable Recommendations", api_error_message)
                    if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"Phase 3 Analysis Error: {api_error_message}"
                    # Clear score fields on error
                    for col in score_cols: df.loc[index, col] = pd.NA

                else:
                    # Populate analysis text fields
                    df.loc[index, 'AI Match Score'] = analysis_results.get("AI Match Score", pd.NA)
                    df.loc[index, 'AI Score Justification'] = analysis_results.get("AI Score Justification", "N/A")
                    df.loc[index, 'AI Strengths'] = analysis_results.get("AI Strengths", "N/A")
                    df.loc[index, 'AI Areas for Improvement'] = analysis_results.get("AI Areas for Improvement", "N/A")
                    df.loc[index, 'AI Actionable Recommendations'] = analysis_results.get("AI Actionable Recommendations", "N/A")

                    # Populate NEW score breakdown columns
                    df.loc[index, 'Keyword Match Score'] = analysis_results.get("Keyword Match Score", pd.NA)
                    df.loc[index, 'Achievements Score'] = analysis_results.get("Achievements Score", pd.NA)
                    df.loc[index, 'Summary Quality Score'] = analysis_results.get("Summary Quality Score", pd.NA)
                    df.loc[index, 'Structure Score'] = analysis_results.get("Structure Score", pd.NA)
                    df.loc[index, 'Tools Certs Score'] = analysis_results.get("Tools Certs Score", pd.NA)

                    # --- Calculate Total Match Score ---
                    scores_to_sum = [
                        df.loc[index, 'Keyword Match Score'],
                        df.loc[index, 'Achievements Score'],
                        df.loc[index, 'Summary Quality Score'],
                        df.loc[index, 'Tools Certs Score']
                    ]
                    # Ensure numeric type before summing, handle NAs
                    numeric_scores = [pd.to_numeric(s, errors='coerce') for s in scores_to_sum]
                    valid_scores = [s for s in numeric_scores if pd.notna(s)]

                    if valid_scores: # Check if there are any valid scores to sum
                        total_score = sum(valid_scores)
                        df.loc[index, 'Total Match Score'] = total_score
                    else:
                        df.loc[index, 'Total Match Score'] = pd.NA # Assign NA if all components were NA

                    logging.info(f"  Successfully stored analysis results (Overall Score: {df.loc[index, 'AI Match Score']}, Total Score for Threshold: {df.loc[index, 'Total Match Score']}).")

            # --- Final Status Update ---
            if analysis_step_successful:
                df.loc[index, 'Status'] = status_analyzed
                update_count += 1
                logging.info(f"  SUCCESS - Row {index+1}. Status: '{status_analyzed}'.")
            else:
                logging.error(f"  FAILURE - Row {index+1}. Status: '{df.loc[index, 'Status']}'. Error: {api_error_message}")
                if 'Notes' in df.columns and api_error_message and "Phase 3" not in str(df.loc[index, 'Notes']):
                     df.loc[index, 'Notes'] = f"Phase 3 Failed: {api_error_message}"

            # --- Periodic Save ---
            if processed_in_run % save_interval == 0:
                batch_time = time.time() - batch_start_time
                logging.info(f"Processed {processed_in_run} rows in AI batch ({batch_time:.2f} sec). Saving progress...")
                try:
                    # Fill NA before save for cleaner Excel output
                    df_save_progress = df.fillna('') # Use fillna('') for general save
                    df_save_progress.to_excel(excel_filepath, index=False, engine='openpyxl')
                    logging.info("Progress saved successfully.")
                    batch_start_time = time.time()
                except PermissionError: logging.error(f"PERM ERROR saving progress: {excel_filepath}. Stopping."); return False
                except Exception as save_err: logging.error(f"Error saving progress: {save_err}"); logging.warning("Continuing...")

        # --- Final Save After Loop ---
        logging.info("Finished AI Analysis loop. Performing final save...")
        try:
             # Ensure final DataFrame uses the standard column order
             df_final = df.reindex(columns=ALL_EXPECTED_COLUMNS)
             # Fill remaining NA with empty strings for better Excel readability
             # This converts score pd.NA to '', check if acceptable
             df_final = df_final.fillna('')

             df_final.to_excel(excel_filepath, index=False, engine='openpyxl')
             logging.info("Final Excel file saved successfully.")
        except PermissionError: logging.error(f"FINAL SAVE ERROR: Permission denied: {excel_filepath}."); return False
        except Exception as save_err: logging.error(f"Error during final save: {save_err}", exc_info=True); return False

        logging.info(f"Phase 3 finished. Successfully analyzed {update_count} out of {num_to_process} targeted rows.")
        return True

    except FileNotFoundError: logging.error(f"Excel file not found: '{excel_filepath}'."); return False
    except KeyError as e: logging.error(f"Missing key in config/DataFrame: {e}", exc_info=True); return False
    except Exception as e: logging.critical(f"Crit error during Phase 3 setup/processing: {e}", exc_info=True); return False

# --- Main Function Wrapper for Phase 3 ---
def run_phase3_ai_processing(config: dict) -> bool:
    """Executes Phase 3: Load API key/resume HTML, extract text, configure Gemini, process Excel, save."""
    logging.info("Initiating Phase 3: AI Analysis & Scoring")
    overall_success = False

    # --- 1. Load API Key and Resume HTML ---
    api_key, resume_html_content = load_api_key_and_resume_html(config)
    if not api_key: logging.critical("API Key loading failed."); return False
    if not resume_html_content: logging.critical("Resume HTML content loading failed."); return False

    # --- 2. Extract Plain Text from Resume HTML ---
    resume_plain_text = extract_text_from_html(resume_html_content)
    if not resume_plain_text: logging.critical("Failed to extract plain text from Resume HTML."); return False

    # --- 3. Configure Gemini ---
    if not configure_gemini(api_key, config): logging.critical("Gemini API configuration failed."); return False

    # --- 4. Process Excel ---
    try:
        overall_success = process_ai_analysis(config, resume_plain_text)
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in run_phase3: {e}", exc_info=True)
        overall_success = False

    if overall_success: logging.info("Phase 3 processing run completed.")
    else: logging.error("Phase 3 processing run finished with critical errors or failures.")

    return overall_success

# No `if __name__ == "__main__":` block needed as it's run by main_workflow.py