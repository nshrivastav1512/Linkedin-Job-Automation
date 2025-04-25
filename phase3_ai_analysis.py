# phase3_ai_analysis.py
# Phase 3: Uses AI (Gemini) to analyze job descriptions and compare them with the resume.
# Reads Resume HTML, extracts text, performs analysis, adds detailed scores, formats text outputs.

import os
import time
import traceback
import re
import json
import logging
import random
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union, Any # Use Union for type hints
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Attempt to import AI library
try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
except ImportError:
    logging.critical("CRITICAL ERROR: google-generativeai library not installed. Run 'pip install google-generativeai'")
    raise

# --- Column Definitions ---
# Ensure this list is kept synchronized across all phase files!
ALL_EXPECTED_COLUMNS = [
    # Core Job Info
    'Job ID', 'Title', 'Company', 'Location', 'Workplace Type', 'Link', 'Easy Apply', 'Promoted', 'Viewed',
    'Early Applicant', 'Verified', 'Posted Ago Text', 'Posted Days Ago', 'Posted Hours Ago', 'Salary Range',
    'Insights', 'Source', 'Date Added', 'Status', 'Applied Date', 'Notes', 'Applicant Count',
    # Phase 2 Detailed Info
    'Job Description HTML', 'Job Description Plain Text', 'About Company', 'Date Scraped Detailed',
    'Posted Ago Text Detailed', 'Company LinkedIn URL', 'Company Industry', 'Company Size',
    'Company LinkedIn Members', 'Company Followers', 'Hiring Team Member 1 Name',
    'Hiring Team Member 1 Profile URL', 'Scraping Issues',
    # Phase 3 - AI Extracted Text Outputs
    'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills',
    'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description',
    # Phase 3 - AI Analysis Outputs
    'AI Match Score', 'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement',
    'AI Actionable Recommendations',
    # Phase 3 - Detailed Score Columns
    'Keyword Match Score', 'Achievements Score', 'Summary Quality Score', 'Structure Score',
    'Tools Certs Score', 'Total Match Score',
    # Phase 4 Tailoring Output
    'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List',
    'Tailored HTML Path', 'Tailored PDF Path', 'Tailored PDF Pages',
    # Phase 5 Rescoring Output
    'Tailored Resume Score', 'Score Change', 'Tailoring Effectiveness Status', 'Retailoring Attempts'
]
PHASE_3_OUTPUT_COLUMNS = [
    'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills',
    'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description',
    'AI Match Score', 'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement',
    'AI Actionable Recommendations', 'Keyword Match Score', 'Achievements Score',
    'Summary Quality Score', 'Structure Score', 'Tools Certs Score', 'Total Match Score'
]

# --- Helper Functions ---

def load_api_key(config: dict) -> Union[str, None]:
    """Loads the Gemini API key from the .env file."""
    api_key = None
    try:
        env_path = config['paths']['env_filepath']; api_key_name = config['ai']['api_key_name']
        logging.info(f"Loading API key '{api_key_name}' from: {env_path}")
        if env_path.exists():
            load_dotenv(dotenv_path=env_path); api_key = os.getenv(api_key_name)
            if not api_key: logging.error(f"API Key '{api_key_name}' not found in {env_path}.")
            else: logging.info(f"API Key '{api_key_name}' loaded successfully.")
        else: logging.error(f".env file not found at {env_path}.")
    except Exception as e: logging.error(f"Error loading API key: {e}", exc_info=True)
    return api_key

def load_base_resume_html(config: dict) -> Union[str, None]:
    """Loads the base resume HTML content from the specified file."""
    resume_html_content = None
    try:
        resume_html_path = config['paths']['resume_filepath_html']
        logging.info(f"Reading base resume HTML file from: {resume_html_path}")
        if not isinstance(resume_html_path, Path): resume_html_path = Path(resume_html_path)
        if not resume_html_path.is_file(): raise FileNotFoundError(f"Base resume HTML not found: '{resume_html_path}'.")
        with open(resume_html_path, 'r', encoding='utf-8') as f: resume_html_content = f.read()
        if not resume_html_content: logging.error(f"Base resume HTML file '{resume_html_path}' is empty."); resume_html_content = None
        else: logging.info(f"Loaded base resume HTML content (length: {len(resume_html_content)} chars).")
    except FileNotFoundError as fnf_err: logging.error(fnf_err); resume_html_content = None
    except Exception as e: logging.error(f"Failed to read base resume HTML: {e}", exc_info=True); resume_html_content = None
    return resume_html_content

def strip_html_tags(html_text: str) -> str:
    if not html_text or not isinstance(html_text, str): return ""
    try: soup = BeautifulSoup(html_text, 'html.parser'); return soup.get_text(separator=' ', strip=True)
    except Exception: 
        try: return re.sub(r'<[^>]+>', ' ', html_text).strip(); 
        except: return html_text

def format_list_as_bullets(data_input: Union[list, str, pd.Series, np.ndarray, None], indent="- ") -> str:
    if data_input is None: return "N/A"
    items_to_format = []
    if isinstance(data_input, (list, tuple)): items_to_format = list(data_input)
    elif isinstance(data_input, (pd.Series, np.ndarray)): items_to_format = [item for item in data_input if pd.notna(item) and item is not None]
    elif isinstance(data_input, str):
        try: parsed_list = json.loads(data_input); items_to_format = parsed_list if isinstance(parsed_list, list) else [str(parsed_list)]
        except json.JSONDecodeError: items_to_format = [data_input] if data_input.strip() else []
    else: items_to_format = [str(data_input)] if pd.notna(data_input) else []
    if not items_to_format: return "N/A"
    cleaned_items = [strip_html_tags(str(item)).strip() for item in items_to_format]
    valid_items = [item for item in cleaned_items if item]
    return "\n".join([f"{indent}{item}" for item in valid_items]) if valid_items else "N/A"

def extract_text_from_html(html_content: str) -> Union[str, None]:
    if not html_content: logging.error("Cannot extract text from empty HTML."); return None
    logging.debug("Extracting plain text from HTML content...")
    try:
        soup = BeautifulSoup(html_content, 'html.parser'); container = soup.find('div', class_='container') or soup.body or soup
        if container:
            for tag in container(["script", "style"]): tag.decompose()
            text = container.get_text(separator='\n', strip=True); text = re.sub(r'\n{3,}', '\n\n', text)
            logging.debug(f"Extracted text length: {len(text)}")
            return text
        else: logging.error("Could not find main container in HTML."); return "Error: Could not find main content"
    except Exception as e: logging.error(f"Error extracting text from HTML: {e}"); return f"Error extracting text: {e}"

def configure_gemini(api_key: str, config: dict) -> bool:
    if not api_key: logging.error("Cannot configure Gemini: API key missing."); return False
    try: genai.configure(api_key=api_key); logging.info("Gemini API configured successfully."); return True
    except Exception as e: logging.error(f"Failed to configure Gemini API: {e}", exc_info=True); return False

def parse_safety_settings(config: dict) -> Union[dict, None]:
    settings_dict = config['ai'].get('safety_settings', {}); parsed_settings = {}
    try:
        for key_str, value_str in settings_dict.items():
            cat = getattr(HarmCategory, key_str, None); thr = getattr(HarmBlockThreshold, value_str, None)
            if cat and thr: parsed_settings[cat] = thr
            else: logging.warning(f"Invalid safety setting: {key_str}={value_str}. Skipping.")
        return parsed_settings
    except Exception as e: logging.error(f"Error parsing safety settings: {e}"); return None

def call_gemini_api(model_name: str, prompt_text: str, config: dict, is_json_output: bool, attempt=1, max_attempts=3) -> Union[dict, str]:
    api_delay = config['ai']['api_delay_seconds']; gen_cfg_key = 'generation_config_json' if is_json_output else 'generation_config_text'
    gen_cfg = config['ai'].get(gen_cfg_key, {}); safety = parse_safety_settings(config)
    logging.info(f"Calling Gemini model '{model_name}' (Attempt {attempt}/{max_attempts}). Expecting {'JSON' if is_json_output else 'Text'}.")
    try:
        model = genai.GenerativeModel(model_name); response = model.generate_content(prompt_text, generation_config=gen_cfg, safety_settings=safety)
        if response.prompt_feedback and response.prompt_feedback.block_reason:
            err = f"ERROR: Prompt blocked ({response.prompt_feedback.block_reason.name})"; return {"error": err} if is_json_output else err
        if not response.candidates: err = "ERROR: No response candidates"; return {"error": err} if is_json_output else err
        cand = response.candidates[0]; finish_reason = getattr(getattr(cand, 'finish_reason', None), 'name', 'UNKNOWN')
        if finish_reason != 'STOP':
            err = f"ERROR: Output truncated ({finish_reason})." if finish_reason == 'MAX_TOKENS' else f"ERROR: Response stopped unexpectedly ({finish_reason})"
            try: part = response.text
            except Exception: part = None
            if is_json_output: return {"error": err, "partial_response": part}
            else: return err + (f"\nPartial Text:\n{part}" if part else "")
        try:
            resp_txt = response.text; logging.info("Gemini API call successful.")
            if is_json_output:
                try:
                    clean_txt = re.sub(r'^```json\s*|\s*```$', '', resp_txt, flags=re.M|re.S).strip()
                    if not clean_txt: return {"error": "Empty JSON response"}
                    parsed_json = json.loads(clean_txt)
                    if isinstance(parsed_json, list) and len(parsed_json) > 0 and isinstance(parsed_json[0], dict):
                        logging.warning("AI list->dict fix: Using first element."); parsed_json = parsed_json[0]
                    if not isinstance(parsed_json, dict): logging.error(f"Parsed JSON is not dict: {type(parsed_json)}"); return {"error": "Parsed response is not JSON object", "raw_response": resp_txt}
                    return parsed_json
                except Exception as json_e: logging.error(f"Failed JSON decode: {json_e}. Raw:\n{resp_txt[:500]}..."); return {"error": "Failed to parse JSON", "raw_response": resp_txt}
            else: return resp_txt
        except Exception as ve: err = f"ERROR: No text content (Finish Reason: {finish_reason})"; return {"error": err} if is_json_output else err
    except Exception as e:
        logging.error(f"Error during Gemini API call: {type(e).__name__} - {e}", exc_info=(attempt==max_attempts))
        if ("Resource has been exhausted" in str(e) or "429" in str(e) or "503" in str(e)) and attempt < max_attempts:
            wait = api_delay * (1.5 ** attempt) + random.uniform(0, 1); logging.warning(f"API busy/rate limit. Retrying in {wait:.1f}s..."); time.sleep(wait)
            return call_gemini_api(model_name, prompt_text, config, is_json_output, attempt + 1, max_attempts)
        err = "ERROR: Invalid Gemini API Key." if "API key not valid" in str(e) else f"ERROR: API Call Failed - {type(e).__name__}"
        return {"error": err} if is_json_output else err

def extract_job_details_with_gemini(jd_plain_text: str, config: dict) -> dict:
    if not jd_plain_text or pd.isna(jd_plain_text) or len(jd_plain_text) < 50: return {"error": "Invalid job description text"}
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
    model_name = config['ai']['extraction_model_name']; response_data = call_gemini_api(model_name, prompt, config, is_json_output=True)
    if isinstance(response_data, dict) and "error" in response_data: logging.error(f"AI extraction failed: {response_data['error']}")
    elif not isinstance(response_data, dict): logging.error(f"AI extraction returned unexpected type: {type(response_data)}."); return {"error": "Invalid response format", "raw_response": str(response_data)}
    else: logging.info("AI extraction successful (returned JSON dictionary).")
    return response_data

def analyze_resume_fit_with_gemini(resume_plain_text: str, jd_plain_text: str, config: dict) -> dict:
    if not resume_plain_text or len(resume_plain_text) < 100: return {"error": "Invalid resume text"}
    if not jd_plain_text or len(jd_plain_text) < 50: return {"error": "Invalid job description text"}
    logging.info("Preparing prompt for Resume Fit Analysis (AI Call 2 - Expecting Text)...")
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
    model_name = config['ai']['analysis_model_name']; response_text = call_gemini_api(model_name, prompt, config, is_json_output=False)
    analysis_results = {
        "AI Match Score": pd.NA, "Rating Category": "N/A", "AI Strengths": "N/A", "AI Areas for Improvement": "N/A",
        "AI Actionable Recommendations": "N/A", "Keyword Match Score": pd.NA, "Achievements Score": pd.NA,
        "Summary Quality Score": pd.NA, "Structure Score": pd.NA, "Tools Certs Score": pd.NA,
        "_full_response": response_text, "_parse_successful": False, "error": None
    }
    if isinstance(response_text, dict) and "error" in response_text: analysis_results["error"] = response_text['error']; analysis_results["AI Actionable Recommendations"] = response_text['error']; return analysis_results
    elif not isinstance(response_text, str) or response_text.startswith("ERROR:"): err_msg = response_text if isinstance(response_text, str) else "Unknown API Error"; analysis_results["error"] = err_msg; analysis_results["AI Actionable Recommendations"] = err_msg; return analysis_results
    try:
        logging.info("Parsing AI analysis response...")
        rating_match = re.search(r"Overall Star Rating:\s*([\d\.]+)\s*out of 5 Stars?\s*\((.*?)\)", response_text, re.IGNORECASE | re.MULTILINE)
        if rating_match:
            try: analysis_results["AI Match Score"] = float(rating_match.group(1).strip())
            except ValueError: analysis_results["AI Match Score"] = pd.NA
            analysis_results["Rating Category"] = rating_match.group(2).strip()
        else: logging.warning("Could not parse 'Overall Star Rating' line.")
        def extract_section(header, text):
            start_match = re.search(rf"^{re.escape(header)}:?\s*$", text, re.I | re.M); start_index = start_match.end() if start_match else -1; end_index = len(text)
            if start_index == -1: return None
            next_headers = ["Strengths:", "Areas for Improvement:", "Actionable Recommendations:", "Evaluation Breakdown:"]
            for next_header in next_headers:
                if next_header.lower() != header.lower():
                    next_match = re.search(rf"^{re.escape(next_header)}:?\s*$", text[start_index:], re.I | re.M)
                    if next_match: end_index = min(end_index, start_index + next_match.start())
            section_text = text[start_index:end_index].strip(); return re.sub(r'^\s*[-*]\s*', '', section_text, flags=re.M).strip() if section_text else None
        analysis_results["AI Strengths"] = extract_section("Strengths", response_text) or "Parsing Error"
        analysis_results["AI Areas for Improvement"] = extract_section("Areas for Improvement", response_text) or "Parsing Error"
        analysis_results["AI Actionable Recommendations"] = extract_section("Actionable Recommendations", response_text) or "Parsing Error" # Initial extract
        logging.debug("  Parsing Evaluation Breakdown scores and justifications...")
        breakdown_details = {1: "1. Keyword Match: N/A", 2: "2. Achievements: N/A", 3: "3. Summary Quality: N/A", 4: "4. Structure: N/A", 5: "5. Tools/Certs: N/A"}
        score_column_keys = {1: "Keyword Match Score", 2: "Achievements Score", 3: "Summary Quality Score", 4: "Structure Score", 5: "Tools Certs Score"}
        breakdown_section_match = re.search(r"Evaluation Breakdown:(.*)", response_text, re.I | re.S); breakdown_search_area = breakdown_section_match.group(1).strip() if breakdown_section_match else ""
        if not breakdown_search_area: logging.error("Could not find 'Evaluation Breakdown:' section.")
        else:
            logging.debug("Found 'Evaluation Breakdown:' section. Parsing criteria..."); found_any_breakdown = False
            score_pattern_template = r"^{num}\.\s*.*?:\s*([\d\.]+)★?(.*)$" # Corrected pattern
            for num in range(1, 6):
                pattern = score_pattern_template.format(num=num)
                match = re.search(pattern, breakdown_search_area, re.I | re.M)
                if match:
                    found_any_breakdown = True; full_matched_line = match.group(0).strip(); score_str = match.group(1).strip(); score_col_key = score_column_keys[num]
                    try: analysis_results[score_col_key] = float(score_str); logging.debug(f"    Parsed {score_col_key}: {analysis_results[score_col_key]}")
                    except ValueError: logging.warning(f"    Could not parse float for {score_col_key} from '{score_str}'"); analysis_results[score_col_key] = pd.NA
                    breakdown_details[num] = full_matched_line; logging.debug(f"    Stored Detail Line {num}: {full_matched_line}")
                else: logging.warning(f"    Pattern did not match for criteria number {num} within breakdown.")
            if not found_any_breakdown: logging.error("Failed to parse any numbered breakdown lines.")
        evaluation_breakdown_text = f"Evaluation Breakdown:\n{breakdown_details[1]}\n{breakdown_details[2]}\n{breakdown_details[3]}\n{breakdown_details[4]}\n{breakdown_details[5]}".strip()
        analysis_results["AI Actionable Recommendations"] = f"{analysis_results['AI Actionable Recommendations']}\n\n{evaluation_breakdown_text}".strip()
        score_part = f"Score: {analysis_results.get('AI Match Score', 'N/A')} ({analysis_results.get('Rating Category', 'N/A')})"
        strengths_part = f"Strengths:\n{analysis_results.get('AI Strengths', 'N/A')}"
        areas_part = f"Areas for Improvement:\n{analysis_results.get('AI Areas for Improvement', 'N/A')}"
        recommendations_part = f"Actionable Recommendations:\n{analysis_results.get('AI Actionable Recommendations', 'N/A')}" # Includes breakdown text
        analysis_results["AI Score Justification"] = f"{score_part}\n\n{strengths_part}\n\n{areas_part}\n\n{recommendations_part}".strip()
        # --- ADDED Debug Log ---
        logging.debug(f"DEBUG: Constructed Justification String (len={len(analysis_results['AI Score Justification'])}): {analysis_results['AI Score Justification'][:200]}...")
        # --- END Debug Log ---
        analysis_results["_parse_successful"] = True; logging.info("Finished parsing AI analysis response structure.")
    except Exception as e:
        logging.error(f"Error parsing analysis text: {e}", exc_info=True)
        analysis_results["error"] = f"Parse Error: {e}"
        analysis_results["AI Actionable Recommendations"] = f"ERROR PARSING RESPONSE. Raw Text:\n{response_text[:1000]}..."
    return analysis_results


# --- Main Processing Function for Phase 3 ---
def process_ai_analysis(config: dict, resume_plain_text: str):
    """Reads Excel, runs AI analysis, updates DataFrame with formatted text & scores, saves."""
    # --- Config Extraction ---
    excel_filepath = config['paths']['excel_filepath']
    status_ready = config['status']['READY_FOR_AI']; status_processing = config['status']['PROCESSING_AI']
    status_analyzed = config['status']['AI_ANALYZED']; status_failed_extract = config['status']['FAILED_AI_EXTRACTION']
    status_failed_analyze = config['status']['FAILED_AI_ANALYSIS']; status_missing_data = config['status']['MISSING_DATA']
    phase3_error_statuses = [status_failed_extract, status_failed_analyze, config['status'].get('FAILED_API_CONFIG'), status_missing_data]
    phase3_error_statuses = [s for s in phase3_error_statuses if s]; save_interval = config['ai'].get('save_interval', 5); retry_failed = config['workflow']['retry_failed_phase3']
    # --- End Config Extraction ---

    logging.info(f"Starting AI analysis processing for: {excel_filepath}"); logging.info(f"Retry previously failed rows: {retry_failed}")
    if not resume_plain_text: logging.error("Resume text missing."); return False
    try:
        logging.info("Reading Excel file..."); df = pd.read_excel(excel_filepath, engine='openpyxl', dtype={'Job ID': str}); logging.info(f"Read {len(df)} rows.")
        added_cols = False
        for col in ALL_EXPECTED_COLUMNS:
             if col not in df.columns: logging.warning(f"Adding missing column '{col}'."); added_cols = True; df[col] = pd.NA if 'Score' in col or 'Attempt' in col or 'Pages' in col else ''
        if added_cols: logging.info("Reordering DataFrame columns."); df = df.reindex(columns=ALL_EXPECTED_COLUMNS)

        # --- Start: Explicit Data Type Handling (Using object for text) ---
        numeric_cols = [col for col in ALL_EXPECTED_COLUMNS if 'Score' in col or 'Attempt' in col or 'Pages' in col or 'Ago' in col or 'Applicant Count' in col]
        text_cols = [col for col in ALL_EXPECTED_COLUMNS if col not in numeric_cols]

        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
            else: df[col] = pd.NA

        for col in text_cols:
            if col in df.columns:
                # Fill NA first, then convert to object type
                df[col] = df[col].fillna('').astype(object) # Use object
            else:
                df[col] = '' # Add if missing
                df[col] = df[col].astype(object) # Use object
        bool_cols = ['Easy Apply', 'Promoted', 'Viewed', 'Early Applicant', 'Verified']
        for col in bool_cols:
            if col in df.columns:
                 df[col] = df[col].replace({'TRUE': True, 'FALSE': False, '1': True, 1: True, '0': False, 0: False, '': False}).fillna(False).astype(bool)
            else: df[col] = False
        logging.info("Applied explicit data type conversions (using object for text) and NA fills.")
        # --- End Data Type Handling ---

        statuses_to_process = [status_ready];
        if retry_failed: statuses_to_process.extend(phase3_error_statuses); statuses_to_process = list(set(statuses_to_process))
        logging.info(f"Will process jobs with status in: {statuses_to_process}"); rows_to_process_mask = df['Status'].isin(statuses_to_process)
        rows_to_process_idx = df[rows_to_process_mask].index; num_to_process = len(rows_to_process_idx); logging.info(f"Found {num_to_process} rows matching processing criteria.")
        if num_to_process == 0:
            logging.info("No rows found needing AI analysis.")
            if added_cols:
                try: df.fillna('').to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Saving schema changes.")
                except Exception as save_err: logging.error(f"Error saving schema changes: {save_err}"); return False
            return True

        update_count = 0; processed_in_run = 0; batch_start_time = time.time()
        for index in rows_to_process_idx:
            processed_in_run += 1; job_title = df.loc[index, 'Title']; company_name = df.loc[index, 'Company']; jd_text = str(df.loc[index, 'Job Description Plain Text']).strip()
            logging.info(f"--- Processing Row {index + 1}/{len(df)} (Index: {index}) | Job: '{job_title}' @ '{company_name}' ---"); df.loc[index, 'Status'] = status_processing
            if not jd_text or len(jd_text) < 50: logging.warning(f"Skipping R{index + 1}: Invalid JD text."); df.loc[index, 'Status'] = status_missing_data; continue
            analysis_step_successful = True; api_error_message = ""
            logging.info("  Starting Info Extraction..."); extracted_info_dict = extract_job_details_with_gemini(jd_text, config); time.sleep(config['ai']['api_delay_seconds'])
            if "error" in extracted_info_dict:
                logging.error(f"  Extraction Failed: {extracted_info_dict['error']}"); df.loc[index, 'Status'] = status_failed_extract; api_error_message = extracted_info_dict['error']; analysis_step_successful = False
                if 'raw_response' in extracted_info_dict and 'Notes' in df.columns: df.loc[index, 'Notes'] = f"P3 Extract Err: {api_error_message} Raw:\n{str(extracted_info_dict.get('raw_response',''))[:500]}"
                df.loc[index, ['Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills', 'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description']] = "Extraction Error"
            else:
                df.loc[index, 'Extracted Responsibilities'] = format_list_as_bullets(extracted_info_dict.get("Key Responsibilities")); df.loc[index, 'Extracted Required Skills'] = format_list_as_bullets(extracted_info_dict.get("Required Skills"))
                df.loc[index, 'Extracted Preferred Skills'] = format_list_as_bullets(extracted_info_dict.get("Preferred Skills")); df.loc[index, 'Extracted Key Qualifications'] = format_list_as_bullets(extracted_info_dict.get("Key Qualifications"))
                df.loc[index, 'Extracted Experience Level'] = str(extracted_info_dict.get("Required Experience Level", "N/A")); df.loc[index, 'Extracted Company Description'] = str(extracted_info_dict.get("Concise Company Description", "N/A"))
                logging.info("  Stored extracted info.")
            if analysis_step_successful:
                logging.info("  Starting Resume Fit Analysis..."); analysis_results = analyze_resume_fit_with_gemini(resume_plain_text, jd_text, config); time.sleep(config['ai']['api_delay_seconds'])
                if analysis_results.get("error"):
                    logging.error(f"  Analysis Failed: {analysis_results['error']}"); df.loc[index, 'Status'] = status_failed_analyze; api_error_message = analysis_results['error']; analysis_step_successful = False
                    df.loc[index, 'AI Actionable Recommendations'] = analysis_results.get("AI Actionable Recommendations", api_error_message)
                    if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"P3 Analysis Err: {api_error_message}"
                    for col in numeric_cols: df.loc[index, col] = pd.NA
                else:
                    # --- Assign Justification with Debug Logging ---
                    justification_value = analysis_results.get("AI Score Justification", "N/A")
                    logging.debug(f"DEBUG: Retrieved Justification (Index {index}): {justification_value[:100]}...")
                    df.loc[index, 'AI Score Justification'] = justification_value
                    logging.debug(f"DEBUG: Assigned Justification to DF (Index {index}): {df.loc[index, 'AI Score Justification'][:100]}...")
                    # --- End Debug Logging ---

                    df.loc[index, 'AI Match Score'] = analysis_results.get("AI Match Score", pd.NA);
                    df.loc[index, 'AI Strengths'] = analysis_results.get("AI Strengths", "N/A"); df.loc[index, 'AI Areas for Improvement'] = analysis_results.get("AI Areas for Improvement", "N/A")
                    df.loc[index, 'AI Actionable Recommendations'] = analysis_results.get("AI Actionable Recommendations", "N/A"); df.loc[index, 'Keyword Match Score'] = analysis_results.get("Keyword Match Score", pd.NA)
                    df.loc[index, 'Achievements Score'] = analysis_results.get("Achievements Score", pd.NA); df.loc[index, 'Summary Quality Score'] = analysis_results.get("Summary Quality Score", pd.NA)
                    df.loc[index, 'Structure Score'] = analysis_results.get("Structure Score", pd.NA); df.loc[index, 'Tools Certs Score'] = analysis_results.get("Tools Certs Score", pd.NA)
                    scores_to_sum = [df.loc[index, 'Keyword Match Score'], df.loc[index, 'Achievements Score'], df.loc[index, 'Summary Quality Score'], df.loc[index, 'Tools Certs Score']]
                    valid_scores = [s for s in pd.to_numeric(scores_to_sum, errors='coerce') if pd.notna(s)]
                    df.loc[index, 'Total Match Score'] = sum(valid_scores) if valid_scores else pd.NA
                    logging.info(f"  Stored analysis results (Overall: {df.loc[index, 'AI Match Score']}, Total: {df.loc[index, 'Total Match Score']}).")
            if analysis_step_successful: df.loc[index, 'Status'] = status_analyzed; update_count += 1; logging.info(f"  SUCCESS - Row {index+1}. Status: '{status_analyzed}'.")
            else: logging.error(f"  FAILURE - Row {index+1}. Status: '{df.loc[index, 'Status']}'. Error: {api_error_message}")
            if processed_in_run % save_interval == 0:
                batch_time = time.time() - batch_start_time; logging.info(f"Processed {processed_in_run} rows ({batch_time:.2f} sec). Saving progress...")
                try: df.fillna('').to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Progress saved."); batch_start_time = time.time()
                except PermissionError: logging.error(f"PERM ERROR saving progress: {excel_filepath}. Stopping."); return False
                except Exception as save_err: logging.error(f"Error saving progress: {save_err}"); logging.warning("Continuing...")

        logging.info("Finished AI Analysis loop. Performing final save...");
        try:
             df_final = df.reindex(columns=ALL_EXPECTED_COLUMNS)
             # Force object type for justification before final fillna
             if 'AI Score Justification' in df_final.columns:
                 df_final['AI Score Justification'] = df_final['AI Score Justification'].astype(object)
             df_final = df_final.fillna('') # Fill NA before saving
             df_final.to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Final Excel file saved.")
        except PermissionError: logging.error(f"FINAL SAVE ERROR: Permission denied: {excel_filepath}."); return False
        except Exception as save_err: logging.error(f"Error during final save: {save_err}", exc_info=True); return False
        logging.info(f"Phase 3 finished. Successfully analyzed {update_count} out of {num_to_process} targeted rows."); return True
    except FileNotFoundError: logging.error(f"Excel file not found: '{excel_filepath}'."); return False
    except KeyError as e: logging.error(f"Missing key: {e}", exc_info=True); return False
    except Exception as e: logging.critical(f"Crit error during Phase 3: {e}", exc_info=True); return False


# --- Main Function Wrapper for Phase 3 ---
def run_phase3_ai_processing(config: dict) -> bool:
    """Executes Phase 3: Load API key & resume, extract text, configure Gemini, process Excel."""
    logging.info("Initiating Phase 3: AI Analysis & Scoring")
    overall_success = False
    api_key = load_api_key(config)
    resume_html_content = load_base_resume_html(config)
    if not api_key: logging.critical("API Key loading failed."); return False
    if not resume_html_content: logging.critical("Base Resume HTML loading failed."); return False
    if not configure_gemini(api_key, config): logging.critical("Gemini config failed."); return False
    resume_plain_text = extract_text_from_html(resume_html_content)
    if not resume_plain_text or "Error" in resume_plain_text: logging.critical("Failed text extraction from Base Resume."); return False
    try:
        overall_success = process_ai_analysis(config, resume_plain_text)
    except NameError as ne:
        logging.critical(f"NameError during Phase 3 processing (check variable definitions): {ne}", exc_info=True)
        overall_success = False
    except Exception as e:
        logging.critical(f"Unexpected critical error in run_phase3 processing: {e}", exc_info=True)
        overall_success = False
    if overall_success: logging.info("Phase 3 processing run completed.")
    else: logging.error("Phase 3 processing run finished with critical errors or failures.")
    return overall_success

# No `if __name__ == "__main__":` block needed