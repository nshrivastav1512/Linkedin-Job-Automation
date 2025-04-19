# phase4_tailoring.py
# Phase 4: Tailors the resume HTML/PDF for high-scoring jobs using AI.

import os
import time
import traceback
import re
import json
import logging
import shutil
import copy # Potentially needed if deep copying complex objects
import random # Added for API retry delay if not already present
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Attempt to import necessary libraries, providing guidance on failure
try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold # Keep this for safety settings
except ImportError:
    logging.critical("ERROR: google-generativeai library not found. Run 'pip install google-generativeai'")
    raise
try:
    from weasyprint import HTML as WeasyHTML
    from weasyprint.logger import LOGGER as weasyprint_logger
    # Optional: Reduce WeasyPrint's own logging level if too verbose
    # weasyprint_logger.setLevel(logging.WARNING)
except ImportError:
    logging.critical("ERROR: weasyprint library not found. Run 'pip install weasyprint'. Ensure system dependencies (like GTK+) are installed. See WeasyPrint docs.")
    raise
try:
    from PyPDF2 import PdfReader
except ImportError:
     logging.critical("ERROR: PyPDF2 library not found. Run 'pip install pypdf2'.")
     raise


# Define ALL columns expected (consistency check - Must match phase 1 & 3)
# **IMPORTANT**: Ensure this list matches the one in phase1_list_scraper.py and phase3_ai_analysis.py!
ALL_EXPECTED_COLUMNS = [
    'Job ID', 'Title', 'Company', 'Location', 'Workplace Type', 'Link', 'Easy Apply', 'Promoted', 'Viewed',
    'Early Applicant', 'Verified', 'Posted Ago Text', 'Posted Days Ago', 'Posted Hours Ago', 'Salary Range',
    'Insights', 'Company Logo URL', 'Source', 'Date Added', 'Status', 'Applied Date', 'Notes',
    'Applicant Count', 'Job Description HTML', 'Job Description Plain Text', 'About Company',
    'Date Scraped Detailed', 'Posted Ago Text Detailed', 'Company LinkedIn URL', 'Company Industry',
    'Company Size', 'Company LinkedIn Members', 'Company Followers', 'Hiring Team Member 1 Name',
    'Hiring Team Member 1 Profile URL', 'Hiring Team Member 2 Name', 'Hiring Team Member 2 Profile URL',
    'Skills Required',
    'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills',
    'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description',
    'AI Match Score', 'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement',
    'AI Actionable Recommendations',
    'Keyword Match Score', 'Achievements Score', 'Summary Quality Score', 'Structure Score',
    'Tools Certs Score', 'Total Match Score',
    'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List',
    'Tailored HTML Path', 'Tailored PDF Path'
]
# Columns specifically added/updated by THIS phase
PHASE_4_OUTPUT_COLUMNS = ['Generated Tailored Summary', 'Generated Tailored Bullets',
                          'Generated Tailored Skills List', 'Tailored HTML Path', 'Tailored PDF Path']


# --- Helper Functions ---

def load_api_key(config: dict) -> str | None:
    """Loads API key from .env file specified in config."""
    env_path = config['paths']['env_filepath']
    api_key_name = config['ai']['api_key_name']
    logging.info(f"Loading environment variables from: {env_path}")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path) # Load keys into environment
        api_key = os.getenv(api_key_name)
        if not api_key:
            logging.error(f"{api_key_name} not found in {env_path}.")
            return None
        logging.info(f"{api_key_name} loaded successfully.")
        return api_key
    else:
        logging.error(f".env file not found at {env_path}. Cannot load API key.")
        return None

def configure_gemini(api_key: str, config: dict) -> bool:
    """Configures the Google Gemini API client."""
    if not api_key:
        logging.error("Cannot configure Gemini: API key is missing.")
        return False
    try:
        genai.configure(api_key=api_key)
        logging.info("Gemini API configured successfully.")
        return True
    except Exception as e:
        logging.error(f"Failed to configure Gemini API: {e}", exc_info=True)
        return False

def parse_safety_settings(config: dict) -> dict | None:
    """Parses safety settings from config string values to HarmBlockThreshold enums."""
    settings_dict = config['ai'].get('safety_settings', {})
    parsed_settings = {}
    try:
        for key_str, value_str in settings_dict.items():
            # HarmCategory is directly available after import
            category_enum = getattr(HarmCategory, key_str, None)
            # HarmBlockThreshold is also directly available
            threshold_enum = getattr(HarmBlockThreshold, value_str, None)
            if category_enum and threshold_enum:
                parsed_settings[category_enum] = threshold_enum
            else:
                logging.warning(f"Invalid safety setting in config: {key_str}={value_str}. Skipping.")
        return parsed_settings
    except Exception as e:
        logging.error(f"Error parsing safety settings: {e}", exc_info=True)
        return None

def sanitize_filename(name: str) -> str:
    """Removes illegal characters and replaces spaces for filenames."""
    if not isinstance(name, str): name = 'InvalidName'
    name = re.sub(r'[<>:"/\\|?*]', '_', name) # Replace illegal chars
    name = re.sub(r'\s+', '_', name) # Replace whitespace with underscore
    name = re.sub(r'_+', '_', name) # Collapse multiple underscores
    return name[:100] # Limit length

# --- CORRECTED call_gemini_api function ---
def call_gemini_api(model_name: str, prompt_text: str, config: dict, is_json_output: bool, attempt=1, max_attempts=3) -> dict | str:
    """Makes a call to the Gemini API (FIXED finish_reason check)."""
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
            # Use the block_reason directly (it should be an enum instance)
            reason_name = response.prompt_feedback.block_reason.name
            logging.error(f"Gemini Prompt BLOCKED due to safety settings. Reason: {reason_name}")
            error_msg = f"ERROR: Prompt blocked by safety settings ({reason_name})"
            return {"error": error_msg} if is_json_output else error_msg

        if not response.candidates:
             finish_reason_detail = "N/A"
             try: # Safely try to access finish_reason if candidates exist
                 if hasattr(response, 'candidates') and response.candidates and hasattr(response.candidates[0], 'finish_reason'):
                    finish_reason_detail = response.candidates[0].finish_reason.name
             except Exception: pass
             logging.error(f"Gemini response has no candidates. Finish Reason: {finish_reason_detail}")
             error_msg = f"ERROR: No response candidates (Finish Reason: {finish_reason_detail})"
             return {"error": error_msg} if is_json_output else error_msg

        candidate = response.candidates[0]

        # --- *** CORRECTED FINISH REASON CHECK *** ---
        # Compare the candidate's finish_reason NAME attribute
        candidate_finish_reason = getattr(candidate, 'finish_reason', None)
        finish_reason_name = getattr(candidate_finish_reason, 'name', 'UNKNOWN') # Get name safely

        if finish_reason_name != 'STOP':
        # -------------------------------------------
            logging.warning(f"Gemini response candidate finished with reason: {finish_reason_name}")
            # --- Check specific non-STOP reasons ---
            if finish_reason_name == 'MAX_TOKENS':
                error_msg = "ERROR: Output truncated due to MAX_TOKENS limit."
                try: partial_text = response.text # Attempt to get partial text
                except (ValueError, AttributeError): partial_text = None
                if is_json_output: return {"error": error_msg, "partial_response": partial_text}
                else: return error_msg + (f"\nPartial Text:\n{partial_text}" if partial_text else "")
            else: # Safety, Recitation, Other, Unknown
                 error_msg = f"ERROR: Response stopped unexpectedly (Reason: {finish_reason_name})"
                 return {"error": error_msg} if is_json_output else error_msg

        # --- Access Text Content ---
        try:
            response_text = response.text # This might still fail if content is blocked post-generation but reason wasn't SAFETY
            logging.info("Gemini API call successful.")
            if is_json_output:
                try:
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
             # Check finish reason again *if* accessing .text fails
             final_finish_reason_name = getattr(getattr(candidate, 'finish_reason', None), 'name', 'UNKNOWN')
             logging.error(f"Gemini response generated no valid text content. Finish Reason: {final_finish_reason_name}. Error: {ve}")
             # Check if safety ratings blocked content post-generation
             safety_block_reason = None
             try:
                 if candidate.safety_ratings:
                      for rating in candidate.safety_ratings:
                          if rating.block: safety_block_reason = rating.category.name; break
             except Exception: pass # Ignore errors getting safety ratings

             if safety_block_reason: error_msg = f"ERROR: Content blocked by safety filter ({safety_block_reason})"
             else: error_msg = f"ERROR: Response has no text content (Finish Reason: {final_finish_reason_name})"
             return {"error": error_msg} if is_json_output else error_msg

    except Exception as e:
        logging.error(f"Error during Gemini API call/request: {type(e).__name__} - {e}", exc_info=True)
        # --- Retry Logic (Keep as before) ---
        if ("Resource has been exhausted" in str(e) or "429" in str(e) or "503" in str(e)):
            if attempt < max_attempts:
                wait_time = api_delay * (1.5 ** attempt) + random.uniform(0, 1)
                logging.warning(f"API busy/rate limit. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                return call_gemini_api(model_name, prompt_text, config, is_json_output, attempt + 1, max_attempts)
            else: error_msg = "ERROR: API rate limit/busy after multiple retries"
        elif "API key not valid" in str(e): error_msg = "ERROR: Invalid Gemini API Key."
        else: error_msg = f"ERROR: API Call Failed - {type(e).__name__}" # Capture specific error type
        return {"error": error_msg} if is_json_output else error_msg
# --- END CORRECTED call_gemini_api function ---


def generate_pdf_from_html(html_filepath: Path, pdf_filepath: Path, config: dict) -> bool:
    """Generates a PDF from an HTML file using WeasyPrint."""
    logging.info(f"Generating PDF for: {html_filepath.name} -> {pdf_filepath.name}")
    try:
        if not html_filepath.is_file():
            logging.error(f"HTML file not found for PDF generation: {html_filepath}")
            return False

        # WeasyPrint needs the base URL for relative paths (like CSS if linked)
        base_url_path = html_filepath.resolve().parent
        html = WeasyHTML(filename=html_filepath, base_url=base_url_path.as_uri())
        html.write_pdf(pdf_filepath)
        logging.info(f"Successfully generated PDF: {pdf_filepath.name}")
        return True
    except FileNotFoundError: # Should be caught above, but double check
        logging.error(f"HTML file disappeared before PDF generation: {html_filepath}")
        return False
    except Exception as e:
        # Check for common WeasyPrint dependency issues
        if 'No libraries found for' in str(e) or 'DLL load failed' in str(e) or 'gobject' in str(e) or 'pango' in str(e):
            logging.critical("\n!!!!!! WEASYPRINT SYSTEM DEPENDENCY ERROR !!!!!!")
            logging.critical("Ensure GTK+ runtime or equivalent dependencies are installed and accessible.")
            logging.critical("See WeasyPrint installation documentation for your OS.")
            logging.critical(f"Error details: {e}", exc_info=True)
        else:
            logging.error(f"Error generating PDF from {html_filepath.name}: {type(e).__name__} - {e}", exc_info=True)
        return False

def get_pdf_page_count(pdf_filepath: Path) -> int:
    """Gets the page count of a PDF file. Returns -1 if file not found, -2 on error."""
    logging.info(f"Checking page count for PDF: {pdf_filepath.name}")
    if not pdf_filepath.is_file():
        logging.error(f"PDF file not found for page count check: {pdf_filepath}")
        return -1
    try:
        with open(pdf_filepath, 'rb') as f:
            reader = PdfReader(f)
            count = len(reader.pages)
        logging.info(f"PDF page count: {count}")
        return count
    except Exception as e:
        logging.error(f"Error reading PDF file {pdf_filepath.name} for page count: {e}", exc_info=True)
        return -2 # Indicate error reading file

def extract_text_from_html(html_content: str) -> str:
    """Extracts plain text from HTML resume content for AI input."""
    logging.debug("Extracting base text from HTML template...")
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Try finding a main container, otherwise use body
        container = soup.find('div', class_='container') or soup.body
        if container:
            # Remove script/style tags before extracting text
            for script_or_style in container(["script", "style"]):
                script_or_style.decompose()
            text = container.get_text(separator='\n', strip=True)
            text = re.sub(r'\n{3,}', '\n\n', text) # Consolidate blank lines
            logging.debug(f"Extracted base text length: {len(text)}")
            return text
        else:
            logging.error("Could not find main container (div.container or body) in HTML template.")
            return "Error: Could not find main content in HTML"
    except Exception as e:
        logging.error(f"Error extracting text from HTML: {e}", exc_info=True)
        return f"Error extracting text: {e}"

def strip_html_tags(html_text: str) -> str:
    """Removes HTML tags from a string, used for saving clean text to Excel."""
    if not html_text or not isinstance(html_text, str):
        return ""
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        return soup.get_text(separator=' ', strip=True) # Use space separator
    except Exception as e:
        logging.warning(f"Error stripping HTML tags: {e}", exc_info=False)
        # Return original text with maybe a marker? Or just original? Original is safer.
        return html_text # Return original text on error

def edit_html_with_ai_suggestions(base_html_content: str, ai_json_data: dict) -> tuple[str, bool]:
    """Applies AI's tailored content (summary, bullets, skills) to the base HTML content."""
    logging.info("Applying AI suggestions to HTML structure...")
    try:
        soup = BeautifulSoup(base_html_content, 'html.parser')
        modified = False

        # --- Safely get AI suggestions ---
        tailored_summary = ai_json_data.get('tailored_summary', '')
        relevant_experience_title = str(ai_json_data.get('relevant_experience_title', '') or '').strip()
        tailored_bullets = ai_json_data.get('tailored_bullets', [])
        if not isinstance(tailored_bullets, list): tailored_bullets = []
        skills_dict = ai_json_data.get('skill_categories', {})
        if not isinstance(skills_dict, dict): skills_dict = {}

        # --- 1. Inject Summary ---
        summary_h2 = soup.find(lambda tag: tag.name == 'h2' and 'summary' in tag.get_text(strip=True).lower())
        if summary_h2:
            summary_target_div = summary_h2.find_next_sibling('div')
            if summary_target_div and tailored_summary:
                summary_target_div.clear()
                new_p = soup.new_tag('p')
                new_p.append(BeautifulSoup(tailored_summary, 'html.parser'))
                summary_target_div.append(new_p)
                logging.debug("  Injected tailored summary into HTML.")
                modified = True
            elif tailored_summary:
                logging.warning("  Found 'Summary' heading but no subsequent <div> to inject into.")
        else:
            logging.warning("  Could not find 'Summary' heading (h2) in HTML template.")

        # --- 2. Inject Experience Bullets ---
        exp_h2 = soup.find(lambda tag: tag.name == 'h2' and 'experience' in tag.get_text(strip=True).lower())
        if exp_h2 and relevant_experience_title and tailored_bullets:
            target_ul = None
            possible_h3s = exp_h2.find_all_next('h3')
            for h3_tag in possible_h3s:
                 if relevant_experience_title.lower() in h3_tag.get_text(strip=True).lower():
                     ul_tag = h3_tag.find_next_sibling('ul')
                     if ul_tag: target_ul = ul_tag; break
                     else: logging.warning(f"  Found H3 for '{relevant_experience_title}' but no following <ul>."); break

            if target_ul:
                target_ul.clear()
                for bullet_text in tailored_bullets:
                    if not bullet_text: continue # Skip empty bullets
                    new_li = soup.new_tag('li')
                    new_li.append(BeautifulSoup(bullet_text, 'html.parser'))
                    target_ul.append(new_li)
                logging.debug(f"  Injected {len(tailored_bullets)} tailored bullets for '{relevant_experience_title}'.")
                modified = True
            elif relevant_experience_title:
                 logging.warning(f"  Could not find H3 or its subsequent <ul> for experience title: '{relevant_experience_title}'.")
        elif not relevant_experience_title or not tailored_bullets:
            logging.debug("  Skipping experience bullet injection: No relevant title or bullets provided by AI.")

        # --- 3. Rebuild Skills Section ---
        skills_h2 = soup.find(lambda tag: tag.name == 'h2' and 'skills' in tag.get_text(strip=True).lower())
        if skills_h2 and skills_dict:
            skills_container = skills_h2.find_next_sibling('div', class_='skills-container')
            if skills_container:
                skills_container.clear()
                num_categories = len(skills_dict)
                for category, skills_list in skills_dict.items():
                     if not isinstance(skills_list, list):
                         logging.warning(f"  Skills value for category '{category}' is not a list, skipping.")
                         continue

                     col_div = soup.new_tag('div', **{'class': 'skills-column'})
                     col_h4 = soup.new_tag('h4')
                     col_h4.string = category
                     col_div.append(col_h4)
                     col_ul = soup.new_tag('ul', **{'class': 'skills-list'})

                     for skill_text in skills_list:
                          if not skill_text: continue # Skip empty skills
                          li = soup.new_tag('li')
                          li.append(BeautifulSoup(str(skill_text), 'html.parser'))
                          col_ul.append(li)

                     col_div.append(col_ul)
                     skills_container.append(col_div)

                logging.debug(f"  Rebuilt skills section with {num_categories} categories.")
                modified = True
            else:
                logging.warning("  Found 'Skills' heading but no subsequent 'div.skills-container'.")
        elif skills_dict:
            logging.warning("  Could not find 'Skills' heading (h2) in HTML template to inject skills.")
        else:
            logging.debug("  Skipping skills injection: No skill categories provided by AI.")

        return str(soup), modified

    except Exception as e:
        logging.error(f"Error applying AI suggestions to HTML: {e}", exc_info=True)
        return base_html_content, False # Return original content on error

# --- Iterative Tailoring Function ---
# Note: Updated prompt input logic (using action recommendations)
def iterative_tailoring_and_pdf_gen(
    base_html_content: str,
    base_resume_text_for_ai: str,
    job_data_for_ai: dict, # Pass relevant job data dict
    html_filepath: Path,
    pdf_filepath: Path,
    config: dict
    ) -> tuple[str, dict]:
    """
    Handles the iterative process: AI tailoring -> HTML edit -> PDF gen -> Validation.
    Uses JD, Base Resume Text, and AI Actionable Recommendations as input.
    """
    max_attempts = config['phase4']['max_tailoring_attempts']
    status_flags = config['status']
    tailoring_model = config['ai']['tailoring_model_name']
    api_delay = config['ai']['api_delay_seconds']

    # Status defaults
    final_status = status_flags['FAILED_TAILORING'] # Default to failure
    latest_ai_json_data = {"error": "No successful AI tailoring attempts."} # Store last good JSON

    # Extract data for prompt context (updated for Phase 4 requirements)
    jd_text = job_data_for_ai.get('Job Description Plain Text', '')
    # Use the full recommendations text from Phase 3
    ai_actionable_recommendations_text = job_data_for_ai.get('AI Actionable Recommendations', '')

    for attempt in range(1, max_attempts + 1):
        logging.info(f"--- Tailoring Attempt {attempt}/{max_attempts} ---")

        # --- Construct Prompt ---
        if attempt == 1:
            # Initial prompt uses JD, Base Resume, and Actionable Recommendations
            prompt = f"""
            **Objective:** Generate tailored resume content (Summary, Experience Bullets, Skills) based on the provided Base Resume, Job Description, and AI Analysis/Recommendations. Output must be a single valid JSON object.

            **Inputs:**

            1.  **Base Resume Text Context:** (Used for style, tone, and finding relevant experience)
                ```
                {base_resume_text_for_ai}
                ```

            2.  **Target Job Description:** (Primary source for keywords and requirements)
                ```
                {jd_text}
                ```

            3.  **Previous AI Analysis & Recommendations:** (Highlights areas to address and provides scoring context)
                ```
                {ai_actionable_recommendations_text}
                ```

            **Instructions:**

            1.  **Analyze all inputs:** Understand the target role from the JD, required skills/experience, and guidance from the AI Analysis/Recommendations.
            2.  **Generate Tailored Content:**
                *   `tailored_summary` (String): Craft a concise, impactful summary (2-4 sentences) highlighting the most relevant qualifications from the Base Resume for THIS job, incorporating keywords from the JD and addressing feedback from the Recommendations. Use <strong> tags around key skills/terms from the JD.
                *   `relevant_experience_title` (String): Identify the *single, most relevant* job title heading (e.g., "Software Engineer", "Software Engineer Intern") from the "Base Resume Text Context" provided above that corresponds to the experience you are tailoring bullets for. Use the EXACT title text.
                *   `tailored_bullets` (List of Strings): Rewrite/select 3-5 bullet points for the *identified relevant experience* above. Focus on achievements matching the JD's responsibilities and required skills, guided by the Recommendations. Use action verbs, quantify results where possible, and integrate keywords naturally. Use <strong> tags around important keywords/skills from the JD within the bullets.
                *   `skill_categories` (Dictionary): Create a dictionary where keys are skill category names (e.g., "Languages & Databases", "Tools & Technologies") and values are lists of skill strings highly relevant to the JD, considering guidance from the Recommendations. Prioritize skills mentioned in the JD. Use <strong> tags for skills explicitly required by the JD. Aim for a concise yet comprehensive skills list, potentially combining or omitting less relevant skills from the base resume if needed for space.
            3.  **Guidelines:** Prioritize JD keywords. Include quantifiable metrics if suggested. Use strong action verbs. Keep content concise (target 400-550 words total generated text). Ensure ethical representation.
            4.  **Output Format:** Respond ONLY with a single valid JSON object containing the EXACT keys: "tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories". Do not include explanations or markdown code fences (```json).

            **Generate the JSON object now.**
            """
        elif attempt == 2:
             # Condensation prompt (minor)
             if 'error' in latest_ai_json_data:
                  logging.error("Cannot attempt condensation (Attempt 2) because previous AI call failed.")
                  break
             prompt = f"""
             **Objective:** Condense the previously generated resume TEXT content slightly to help fit onto a single page, while preserving key information, keywords (including <strong> tags), and metrics. Output JSON.

             **Previous Generated Text Content (JSON Format):**
             ```json
             {json.dumps(latest_ai_json_data, indent=2)}
             ```
             **Target Job Description (for context only):**
             ```
             {jd_text[:2000]}...
             ```
             **Instructions:**
             1. Review previous content and JD context.
             2. Make MINOR condensations: Slightly shorten summary/bullets. Rephrase for brevity. Combine very short adjacent bullets IF logical. Do NOT remove entire bullets or skills categories. Preserve <strong> tags. Focus shortening efforts on longer bullets/summary first. Ensure skills list remains clear but concise.
             3. Output ONLY the condensed JSON object with keys: "tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories".

             **Generate the condensed JSON object now.**
             """
        else: # Attempt 3
             # Condensation prompt (major)
             if 'error' in latest_ai_json_data:
                  logging.error("Cannot attempt condensation (Attempt 3) because previous AI call failed.")
                  break
             prompt = f"""
             **Objective:** Significantly shorten the previously generated resume TEXT content to ensure it fits one page. Preserve essential keywords (including <strong> tags) and the most impactful metrics. Output JSON.

             **Previously Condensed Text Content (JSON Format):**
             ```json
             {json.dumps(latest_ai_json_data, indent=2)}
             ```
             **Target Job Description (for context only):**
             ```
             {jd_text[:2000]}...
             ```
             **Instructions:**
             1. Review condensed content and JD context.
             2. Perform SIGNIFICANT shortening: Make summary and bullets much more concise (aim for ~10-20% reduction). Merge/remove less critical bullets, focusing on those least aligned with the JD.
             3. Keep skill categories, but be aggressive in removing less relevant skills or rephrasing for extreme brevity. Preserve essential <strong> tags.
             4. Output ONLY the significantly shortened JSON object with keys: "tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories".

             **Generate the significantly shortened JSON object now.**
             """

        # --- Call Gemini ---
        gemini_response = call_gemini_api(tailoring_model, prompt, config, is_json_output=True, attempt=attempt)
        # No delay here, delay happens *after* call within the function or before next loop iteration

        # --- Process Response ---
        if isinstance(gemini_response, dict) and "error" in gemini_response:
            logging.error(f"Attempt {attempt}: Gemini API call failed: {gemini_response['error']}")
            final_status = f"{status_flags['FAILED_TAILORING']} (API Err Att.{attempt})"[:250] # Truncate error msg if needed
            latest_ai_json_data = gemini_response # Store error dict
            break # Exit loop on API error

        if not isinstance(gemini_response, dict):
             logging.error(f"Attempt {attempt}: Gemini response was not a dictionary as expected. Type: {type(gemini_response)}")
             final_status = f"{status_flags['FAILED_TAILORING']} (Invalid API Resp Att.{attempt})"
             latest_ai_json_data = {"error": "Invalid API response type", "raw_response": str(gemini_response)}
             break

        required_keys = ["tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories"]
        missing_keys = [key for key in required_keys if key not in gemini_response]
        if missing_keys:
            logging.error(f"Attempt {attempt}: AI response missing required keys: {missing_keys}. Response: {gemini_response}")
            final_status = f"{status_flags['FAILED_TAILORING']} (Missing Keys Att.{attempt})"
            latest_ai_json_data = {"error": f"Missing keys: {missing_keys}", "raw_response": gemini_response}
            break

        latest_ai_json_data = gemini_response
        logging.info(f"Attempt {attempt}: Successfully parsed AI suggestions.")

        # --- Edit HTML ---
        edited_html_string, modified_flag = edit_html_with_ai_suggestions(base_html_content, latest_ai_json_data)
        if not modified_flag and attempt == 1:
            logging.warning("Attempt 1: AI suggestions did not result in HTML modifications.")

        # Save edited HTML
        try:
             with open(html_filepath, 'w', encoding='utf-8') as f: f.write(edited_html_string)
             logging.info(f"Attempt {attempt}: Saved edited HTML: {html_filepath.name}")
        except Exception as html_save_err:
             logging.error(f"Attempt {attempt}: ERROR saving edited HTML file {html_filepath.name}: {html_save_err}", exc_info=True)
             final_status = f"{status_flags['FAILED_FILE_ACCESS']} (HTML Save Err Att.{attempt})"
             break

        # --- Generate PDF ---
        pdf_success = generate_pdf_from_html(html_filepath, pdf_filepath, config)
        if not pdf_success:
             final_status = f"{status_flags['FAILED_PDF_GEN']} (Att.{attempt})"
             break

        # --- Validate Page Count ---
        page_count = get_pdf_page_count(pdf_filepath)
        if page_count == 1:
             final_status = status_flags['SUCCESS']
             logging.info(f"Attempt {attempt}: SUCCESS! Generated PDF is 1 page.")
             break
        elif page_count > 1:
             logging.warning(f"Attempt {attempt}: Generated PDF has {page_count} pages. Needs condensation.")
             final_status = status_flags['NEEDS_EDIT']
        else: # page_count < 1
             final_status = f"{status_flags['FAILED_PDF_GEN']} (Validation Err Att.{attempt})"
             logging.error(f"Attempt {attempt}: Error validating PDF file ({page_count=}).")
             break

        # Pause before next API call
        if attempt < max_attempts:
            logging.debug(f"Pausing for {api_delay}s before next tailoring attempt...")
            time.sleep(api_delay)

    # --- Post-Loop: Handle Final Manual Edit Attempt (Keep as before) ---
    if final_status == status_flags['NEEDS_EDIT']:
         logging.warning("Max AI condensation attempts reached, PDF still > 1 page.")
         logging.info("Attempting final measure: Removing last education bullet point...")
         try:
              with open(html_filepath, 'r', encoding='utf-8') as f: current_html = f.read()
              soup = BeautifulSoup(current_html, 'html.parser')
              edu_h2 = soup.find(lambda tag: tag.name == 'h2' and 'education' in tag.get_text(strip=True).lower())
              removed_count = 0
              if edu_h2:
                  # Find all ULs potentially associated with education
                  # Using a more flexible search (next siblings, limited depth)
                  possible_edu_uls = edu_h2.find_next_siblings('ul', limit=5)
                  for edu_ul in possible_edu_uls:
                       # Check if it looks like an education list (contains degree, CGPA etc.) - optional check
                       # if 'CGPA' in edu_ul.get_text() or 'Score' in edu_ul.get_text():
                       last_li = edu_ul.find_all('li', recursive=False)
                       if last_li:
                           logging.debug(f"  Removing last <li> from potential education list: {last_li[-1].get_text(strip=True)[:50]}...")
                           last_li[-1].decompose()
                           removed_count += 1
                           break # Remove only one bullet total

              if removed_count > 0:
                   logging.info(f"Removed last education bullet. Saving modified HTML...")
                   with open(html_filepath, 'w', encoding='utf-8') as f: f.write(soup.prettify())
                   logging.info("Regenerating PDF after final edit...")
                   pdf_success_final = generate_pdf_from_html(html_filepath, pdf_filepath, config)
                   if pdf_success_final:
                        page_count_final = get_pdf_page_count(pdf_filepath)
                        if page_count_final == 1:
                             final_status = status_flags['SUCCESS']
                             logging.info("SUCCESS: Resume is now 1 page after final edit.")
                        else:
                             logging.warning(f"WARN: Resume still {page_count_final} pages after final edit. Status remains '{status_flags['NEEDS_EDIT']}'.")
                   else:
                       final_status = f"{status_flags['FAILED_PDF_GEN']} (Final Edit)"
              else:
                   logging.warning("No education bullets found or removed in final manual edit step.")

         except Exception as final_edit_err:
              logging.error(f"Error during final education bullet removal/PDF regeneration: {final_edit_err}", exc_info=True)
              final_status = f"{status_flags['FAILED_TAILORING']} (Final Edit Err)"

    return final_status, latest_ai_json_data


# --- Main Processing Function for Phase 4 ---
def process_resume_tailoring(config: dict, base_html_content: str, base_resume_text_for_ai: str):
    """Main function to drive the resume tailoring process for eligible jobs."""
    # --- Config Extraction ---
    excel_filepath = config['paths']['excel_filepath']
    output_folder = config['paths']['output_folder']
    status_ready = config['status']['AI_ANALYZED']
    status_tailoring = config['status']['TAILORING']
    status_success = config['status']['SUCCESS']
    status_needs_edit = config['status']['NEEDS_EDIT']
    status_low_score = config['status']['SKIPPED_LOW_SCORE']
    # Use Total Match Score for threshold check
    score_threshold = config['phase4']['score_threshold']
    score_column_to_check = 'Total Match Score' # <-- Use the calculated score column

    save_interval = config['phase4']['save_interval']
    status_flags = config['status']
    retry_failed = config['workflow']['retry_failed_phase4']

    phase4_error_statuses = [
        status_flags.get('FAILED_TAILORING'), status_flags.get('FAILED_HTML_EDIT'),
        status_flags.get('FAILED_PDF_GEN'), status_flags.get('FAILED_FILE_ACCESS'),
        status_flags.get('UNKNOWN_ERROR'), status_needs_edit
    ]
    phase4_error_statuses = [s for s in phase4_error_statuses if s]

    logging.info(f"Starting resume tailoring process for: {excel_filepath}")
    logging.info(f"Retry previously failed/needs-edit rows: {retry_failed}")
    logging.info(f"Using score threshold >= {score_threshold} on column '{score_column_to_check}'")

    try:
        # --- Read/Prepare DataFrame ---
        logging.info("Reading Excel file...")
        df = pd.read_excel(excel_filepath, engine='openpyxl', dtype={'Job ID': str})
        logging.info(f"Read {len(df)} rows.")

        # --- Schema Check & Update ---
        added_cols = False
        # Use the globally defined ALL_EXPECTED_COLUMNS which includes new score cols
        for col in ALL_EXPECTED_COLUMNS:
            if col not in df.columns:
                logging.warning(f"Adding missing column '{col}' to DataFrame.")
                if 'Score' in col: df[col] = pd.NA
                else: df[col] = ''
                added_cols = True
        if added_cols:
            logging.info("Reordering DataFrame columns.")
            df = df.reindex(columns=ALL_EXPECTED_COLUMNS)

        # --- Fill NA/NaN and Convert Types ---
        score_cols_phase4 = ['AI Match Score', 'Keyword Match Score', 'Achievements Score',
                             'Summary Quality Score', 'Structure Score', 'Tools Certs Score', 'Total Match Score']
        for col in score_cols_phase4:
             if col in df.columns:
                  df[col] = pd.to_numeric(df[col], errors='coerce') # Keep as NA if conversion fails
             else: # Should have been added above, but defensive check
                  df[col] = pd.NA

        # Fill other NAs and ensure string types
        text_cols_to_fill_na_phase4 = [
            'Status', 'Job Description Plain Text', 'Company', 'Title',
            'AI Actionable Recommendations', 'Notes' # Add Notes here too
            # Add others if needed
        ]
        for col in text_cols_to_fill_na_phase4:
             if col in df.columns:
                  df[col] = df[col].fillna('N/A').astype(str).replace('nan', 'N/A')
             else:
                  df[col] = 'N/A' # Add column if completely missing

        # --- Fix for Job ID fillna (using the corrected method) ---
        if 'Job ID' not in df.columns: df['Job ID'] = ''
        df['Job ID'] = df['Job ID'].astype(str).replace('nan', '')
        replacement_ids = pd.Series([f"Index{i}" for i in df.index], index=df.index)
        # Use combine_first, replacing empty strings first
        df['Job ID'] = df['Job ID'].replace('', pd.NA).combine_first(replacement_ids)
        df['Job ID'] = df['Job ID'].fillna('').astype(str) # Ensure no NAs remain

        # --- Filter Jobs ---
        logging.info(f"Filtering jobs: (Status='{status_ready}' OR (Retry=True AND Status in {phase4_error_statuses})) AND {score_column_to_check} >= {score_threshold}...")

        ready_mask = (df['Status'] == status_ready)
        # Ensure score comparison handles NA values (treat NA as not meeting threshold)
        score_mask = (df[score_column_to_check].fillna(-1) >= score_threshold) # Fill NA with value below threshold
        retry_mask = pd.Series(False, index=df.index)
        if retry_failed:
            retry_mask = df['Status'].isin(phase4_error_statuses)

        eligible_mask = score_mask & (ready_mask | retry_mask)
        rows_to_process_idx = df[eligible_mask].index
        num_to_process = len(rows_to_process_idx)
        logging.info(f"Found {num_to_process} jobs eligible for tailoring (including retries).")

        # Mark low score jobs only if they were in the 'ready' state initially
        # Ensure low score check also handles NA scores correctly
        low_score_mask = ready_mask & (~score_mask) # Use the same score mask
        low_score_idx = df[low_score_mask].index
        save_needed_for_low_score = False
        if not low_score_idx.empty:
             logging.info(f"Marking {len(low_score_idx)} jobs with status '{status_low_score}' due to low/NA score.")
             df.loc[low_score_idx, 'Status'] = status_low_score
             save_needed_for_low_score = True

        if num_to_process == 0:
            logging.info("No jobs need processing in this phase.")
            if added_cols or save_needed_for_low_score: # Save if needed
                try:
                    logging.info("Saving Excel file due to schema/status changes.")
                    df_save_final = df.fillna('') # Fill NA before save
                    df_save_final.to_excel(excel_filepath, index=False, engine='openpyxl')
                except Exception as save_err: logging.error(f"Error saving changes: {save_err}"); return False
            return True

        # --- Ensure Output Folder Exists ---
        try:
            output_folder.mkdir(parents=True, exist_ok=True)
            logging.info(f"Ensured output directory exists: {output_folder}")
        except Exception as e: logging.critical(f"Could not create output dir: {output_folder}. Error: {e}"); return False

        # --- Main Tailoring Loop ---
        processed_in_run = 0
        success_count = 0
        batch_start_time = time.time()

        for index in rows_to_process_idx:
            processed_in_run += 1
            # Use .get() with defaults for safety when creating job_info
            job_info = {col: df.get(col, pd.Series(dtype=str)).loc[index] for col in df.columns}
            job_title = str(job_info.get('Title', 'UnknownJob'))
            company_name = str(job_info.get('Company', 'UnknownCompany'))
            job_id_for_file = str(job_info.get('Job ID', f"Index{index}"))

            logging.info(f"--- Processing Row {index + 1}/{len(df)} (Index: {index}) | Job: '{job_title}' @ '{company_name}' ---")
            df.loc[index, 'Status'] = status_tailoring

            # Prepare data for the iterative function (Updated Input)
            job_data_for_ai = {
                'Job Description Plain Text': str(job_info.get('Job Description Plain Text', '')),
                'AI Actionable Recommendations': str(job_info.get('AI Actionable Recommendations', 'N/A')), # Pass full recommendations text
            }

            if not job_data_for_ai['Job Description Plain Text'] or len(job_data_for_ai['Job Description Plain Text']) < 50:
                logging.error(f"Skipping row {index+1}: Job Description text missing/too short.")
                df.loc[index, 'Status'] = status_flags['MISSING_DATA']
                continue

            company_sanitized = sanitize_filename(company_name)
            title_sanitized = sanitize_filename(job_title)
            base_filename = f"{company_sanitized}_{title_sanitized}_{job_id_for_file}"
            html_filepath = output_folder / f"{base_filename}.html"
            pdf_filepath = output_folder / f"{base_filename}.pdf"

            try:
                final_status, last_ai_data = iterative_tailoring_and_pdf_gen(
                    base_html_content=base_html_content,
                    base_resume_text_for_ai=base_resume_text_for_ai,
                    job_data_for_ai=job_data_for_ai, # Pass the updated dict
                    html_filepath=html_filepath,
                    pdf_filepath=pdf_filepath,
                    config=config
                )

                df.loc[index, 'Status'] = final_status
                df.loc[index, 'Tailored HTML Path'] = str(html_filepath.resolve()) if html_filepath.exists() else ''
                df.loc[index, 'Tailored PDF Path'] = str(pdf_filepath.resolve()) if pdf_filepath.exists() and final_status != status_flags['FAILED_PDF_GEN'] else ''

                if isinstance(last_ai_data, dict) and 'error' not in last_ai_data:
                    summary_raw = last_ai_data.get('tailored_summary', '')
                    bullets_raw = last_ai_data.get('tailored_bullets', [])
                    skills_dict = last_ai_data.get('skill_categories', {})
                    df.loc[index, 'Generated Tailored Summary'] = strip_html_tags(summary_raw)
                    df.loc[index, 'Generated Tailored Bullets'] = "\n".join([strip_html_tags(b) for b in bullets_raw if b])
                    skills_list_cleaned = []
                    for category, skills in skills_dict.items():
                        if isinstance(skills, list):
                             skills_list_cleaned.extend([f"{category}: {strip_html_tags(s)}" for s in skills if s])
                    df.loc[index, 'Generated Tailored Skills List'] = "\n".join(skills_list_cleaned)
                elif isinstance(last_ai_data, dict) and 'error' in last_ai_data:
                     error_info = f"AI Error: {last_ai_data['error']}"
                     if 'raw_response' in last_ai_data: error_info += f" | Raw: {str(last_ai_data['raw_response'])[:200]}..."
                     df.loc[index, 'Generated Tailored Summary'] = error_info[:1000] # Limit length
                     df.loc[index, 'Generated Tailored Bullets'] = "See Summary/Notes"
                     df.loc[index, 'Generated Tailored Skills List'] = "See Summary/Notes"
                     if 'Notes' in df.columns: df.loc[index, 'Notes'] = error_info

                if final_status == status_success: success_count += 1
                else: logging.warning(f"Tailoring finished for row {index+1} with status: {final_status}")

            except Exception as process_err:
                 logging.error(f"Unexpected error during iterative tailoring for row {index+1}: {process_err}", exc_info=True)
                 df.loc[index, 'Status'] = status_flags['UNKNOWN_ERROR']
                 if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"Phase 4 Loop Error: {process_err}"

            # --- Periodic Save ---
            if processed_in_run % save_interval == 0:
                batch_time = time.time() - batch_start_time
                logging.info(f"Processed {processed_in_run} jobs for tailoring ({batch_time:.2f} sec). Saving progress...")
                try:
                    df_save_progress = df.fillna('') # Fill NA for saving
                    df_save_progress.to_excel(excel_filepath, index=False, engine='openpyxl')
                    logging.info("Progress saved successfully.")
                    batch_start_time = time.time()
                except PermissionError:
                    # --- Improved PermissionError Handling ---
                    logging.error(f"PERMISSION ERROR saving progress to {excel_filepath}. PLEASE CLOSE THE FILE if it is open in Excel or another program. Stopping Phase 4.")
                    return False # Stop the phase cleanly
                except Exception as save_err:
                    logging.error(f"Error saving progress: {save_err}")
                    logging.warning("Continuing processing...") # Decide if other errors are critical

        # --- Final Save After Loop ---
        logging.info("Finished Tailoring loop. Performing final save...")
        try:
             df_final = df.fillna('') # Fill NA before final save
             df_final.to_excel(excel_filepath, index=False, engine='openpyxl')
             logging.info("Final Excel file saved successfully.")
        except PermissionError:
            logging.error(f"FINAL SAVE ERROR: Permission denied for {excel_filepath}. PLEASE CLOSE THE FILE. Some updates may not be saved.")
            return False # Indicate failure if final save fails
        except Exception as save_err:
            logging.error(f"Error during final save: {save_err}", exc_info=True)
            return False # Indicate failure

        logging.info(f"Phase 4 finished. Processed {processed_in_run} jobs. {success_count} PDFs generated as 1 page.")
        return True

    except FileNotFoundError: logging.error(f"Excel file not found: '{excel_filepath}'."); return False
    except KeyError as e: logging.error(f"Missing expected column during setup: {e}", exc_info=True); return False
    except Exception as e: logging.critical(f"Crit error during Phase 4 setup/processing: {e}", exc_info=True); return False


# --- Main Function for Phase 4 ---
def run_phase4_resume_tailoring(config: dict) -> bool:
    """
    Executes the Phase 4 workflow: setup, load template, process jobs, save.
    """
    logging.info("Initiating Phase 4: AI Resume Tailoring & PDF Generation")
    overall_success = False

    # --- 1. Load API Key & Configure Gemini ---
    api_key = load_api_key(config)
    if not api_key or not configure_gemini(api_key, config):
        logging.critical("API Key loading or Gemini configuration failed. Phase 4 cannot proceed.")
        return False

    # --- 2. Load Base HTML Template ---
    html_template_path = config['paths']['resume_filepath_html']
    try:
        logging.info(f"Loading HTML template from: {html_template_path}")
        if not html_template_path.is_file():
             raise FileNotFoundError(f"HTML template file not found: {html_template_path}")
        with open(html_template_path, 'r', encoding='utf-8') as f:
            base_html_template_content = f.read()
        # Extract base text *once* for AI prompts
        base_resume_text_for_ai = extract_text_from_html(base_html_template_content)
        if "Error" in base_resume_text_for_ai or not base_resume_text_for_ai:
            logging.error("Could not extract base text from HTML template. Check template content/structure.")
            return False # Critical failure if template is unusable
        logging.info("Successfully loaded HTML template and extracted base text.")
    except Exception as e:
        logging.critical(f"Failed to load or parse HTML template: {e}", exc_info=True)
        return False

    # --- 3. Process Tailoring ---
    try:
        # Pass base HTML content and extracted text to the processing function
        overall_success = process_resume_tailoring(config, base_html_template_content, base_resume_text_for_ai)
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in run_phase4: {e}")
        logging.critical(traceback.format_exc())
        overall_success = False

    if overall_success:
        logging.info("Phase 4 processing run completed.")
    else:
        logging.error("Phase 4 processing run finished with critical errors or failures.")

    return overall_success

# No `if __name__ == "__main__":` block needed