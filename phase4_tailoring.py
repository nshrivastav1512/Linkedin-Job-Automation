# phase4_tailoring.py
# Phase 4: Tailors the resume HTML/PDF for high-scoring jobs using AI,
#          handles re-tailoring attempts based on Phase 5 feedback.

import os
import time
import traceback
import re
import json
import logging
import shutil
import copy
import random
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from typing import Union, Any

# Attempt to import necessary libraries
try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
except ImportError: logging.critical("ERROR: google-generativeai library not found."); raise
try: from weasyprint import HTML as WeasyHTML; from weasyprint.logger import LOGGER as weasyprint_logger # weasyprint_logger.setLevel(logging.WARNING)
except ImportError: logging.critical("ERROR: weasyprint library not found."); raise
try: from PyPDF2 import PdfReader
except ImportError: logging.critical("ERROR: PyPDF2 library not found."); raise

# Import split functions from Phase 3
try:
    from phase3_ai_analysis import ( load_api_key, load_base_resume_html, configure_gemini,
        extract_text_from_html, call_gemini_api, parse_safety_settings )
except ImportError as e: logging.critical(f"ERROR: Could not import from phase3: {e}"); raise

# --- Column Definitions ---
# Ensure this list is kept synchronized across all phase files!
ALL_EXPECTED_COLUMNS = [
    'Job ID', 'Title', 'Company', 'Location', 'Workplace Type', 'Link', 'Easy Apply', 'Promoted', 'Viewed',
    'Early Applicant', 'Verified', 'Posted Ago Text', 'Posted Days Ago', 'Posted Hours Ago', 'Salary Range',
    'Insights', 'Source', 'Date Added', 'Status', 'Applied Date', 'Notes', 'Applicant Count',
    'Job Description HTML', 'Job Description Plain Text', 'About Company', 'Date Scraped Detailed',
    'Posted Ago Text Detailed', 'Company LinkedIn URL', 'Company Industry', 'Company Size',
    'Company LinkedIn Members', 'Company Followers', 'Hiring Team Member 1 Name',
    'Hiring Team Member 1 Profile URL', 'Scraping Issues', 'Extracted Responsibilities',
    'Extracted Required Skills', 'Extracted Preferred Skills', 'Extracted Experience Level',
    'Extracted Key Qualifications', 'Extracted Company Description', 'AI Match Score',
    'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement', 'AI Actionable Recommendations',
    'Keyword Match Score', 'Achievements Score', 'Summary Quality Score', 'Structure Score',
    'Tools Certs Score', 'Total Match Score', 'Generated Tailored Summary',
    'Generated Tailored Bullets', 'Generated Tailored Skills List', 'Tailored HTML Path',
    'Tailored PDF Path', 'Tailored PDF Pages', 'Tailored Resume Score', 'Score Change',
    'Tailoring Effectiveness Status', 'Retailoring Attempts'
]
PHASE_4_OUTPUT_COLUMNS = [
    'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List',
    'Tailored HTML Path', 'Tailored PDF Path', 'Tailored PDF Pages', 'Retailoring Attempts'
]


# --- Helper Functions ---
# (Imported from phase3: load_api_key, load_base_resume_html, configure_gemini, extract_text_from_html, call_gemini_api, parse_safety_settings)

def sanitize_filename(name: str) -> str:
    if not isinstance(name, str): name = 'InvalidName'
    name = re.sub(r'[<>:"/\\|?*]', '_', name); name = re.sub(r'\s+', '_', name)
    name = re.sub(r'_+', '_', name); return name[:100]

def generate_pdf_from_html(html_filepath: Path, pdf_filepath: Path, config: dict) -> bool:
    logging.info(f"Generating PDF for: {html_filepath.name} -> {pdf_filepath.name}")
    try:
        if not html_filepath.is_file(): logging.error(f"HTML file not found for PDF gen: {html_filepath}"); return False
        base_url = html_filepath.resolve().parent.as_uri(); html = WeasyHTML(filename=html_filepath, base_url=base_url)
        html.write_pdf(pdf_filepath); logging.info(f"Successfully generated PDF: {pdf_filepath.name}"); return True
    except Exception as e:
        if 'No libraries found for' in str(e) or 'DLL load failed' in str(e) or 'gobject' in str(e): logging.critical("\n!!!!!! WEASYPRINT SYSTEM DEPENDENCY ERROR !!!!!!\n")
        logging.error(f"Error generating PDF from {html_filepath.name}: {e}", exc_info=True); return False

def get_pdf_page_count(pdf_filepath: Path) -> int:
    logging.info(f"Checking page count for PDF: {pdf_filepath.name}")
    if not pdf_filepath.is_file(): logging.error(f"PDF file not found: {pdf_filepath}"); return -1
    try:
        with open(pdf_filepath, 'rb') as f: reader = PdfReader(f); count = len(reader.pages)
        logging.info(f"PDF page count: {count}"); return count
    except Exception as e: logging.error(f"Error reading PDF {pdf_filepath.name}: {e}"); return -2

def strip_html_tags(html_text: str) -> str:
    if not html_text or not isinstance(html_text, str): return ""
    try: soup = BeautifulSoup(html_text, 'html.parser'); return soup.get_text(separator=' ', strip=True)
    except Exception as e: logging.warning(f"Error stripping HTML: {e}", exc_info=False); return html_text

# Corrected based on user's Resume.html structure
def edit_html_with_ai_suggestions(base_html_content: str, ai_json_data: dict) -> tuple[str, bool]:
    """Applies AI's tailored content (summary, bullets, skills) to the base HTML content."""
    logging.info("Applying AI suggestions to HTML structure...")
    if not isinstance(ai_json_data, dict): logging.error("Invalid AI data for HTML editing."); return base_html_content, False
    try:
        soup = BeautifulSoup(base_html_content, 'html.parser'); modified = False
        tailored_summary = ai_json_data.get('tailored_summary', ''); relevant_experience_title = str(ai_json_data.get('relevant_experience_title', '') or '').strip()
        tailored_bullets = ai_json_data.get('tailored_bullets', []); skills_dict = ai_json_data.get('skill_categories', {})
        if not isinstance(tailored_bullets, list): tailored_bullets = []
        if not isinstance(skills_dict, dict): skills_dict = {}

        # --- 1. Inject Summary ---
        summary_h2 = soup.find(lambda t: t.name == 'h2' and 'summary' in t.get_text(strip=True).lower())
        if summary_h2 and tailored_summary:
            logging.debug("  Found 'Summary' H2.")
            summary_section_div = summary_h2.find_parent('div', class_='section')
            if summary_section_div:
                logging.debug("  Found parent 'div.section' for Summary.")
                h2_tag = summary_section_div.find('h2') # Keep the h2
                summary_section_div.clear() # Clear everything inside the section
                if h2_tag: summary_section_div.append(h2_tag) # Add h2 back
                new_p = soup.new_tag('p'); new_p.append(BeautifulSoup(tailored_summary, 'html.parser'))
                summary_section_div.append(new_p) # Append the new summary paragraph
                logging.debug("  Cleared section and injected H2 + tailored summary <p>."); modified = True
            else: logging.warning("  Could not find parent 'div.section' for 'Summary' H2.")
        elif not tailored_summary: logging.debug("  No tailored summary provided.")
        else: logging.warning("  Could not find 'Summary' heading (h2).")

        # --- 2. Inject Experience Bullets ---
        exp_h2 = soup.find(lambda t: t.name == 'h2' and 'experience' in t.get_text(strip=True).lower())
        if exp_h2 and relevant_experience_title and tailored_bullets:
            logging.debug(f"  Found 'Experience' H2. Searching for H3 containing: '{relevant_experience_title}'")
            target_ul = None
            for h3_tag in exp_h2.find_all_next(['h3', 'h2']):
                 if h3_tag.name == 'h2': break
                 h3_text = h3_tag.get_text(strip=True).lower()
                 # Check if the AI title *is contained within* the H3 text (Company Name)
                 if relevant_experience_title.lower() in h3_text:
                     logging.debug(f"    Found matching H3: '{h3_tag.get_text(strip=True)}'")
                     parent_clearfix_div = h3_tag.find_parent('div', class_='clearfix')
                     if parent_clearfix_div:
                          ul_tag = parent_clearfix_div.find_next_sibling('ul') # The UL is the sibling of the div.clearfix
                          if ul_tag: logging.debug(f"    Found subsequent UL."); target_ul = ul_tag
                          else: logging.warning(f"    Found H3 parent div but no immediate sibling <ul>.")
                     else: logging.warning(f"    Could not find parent 'div.clearfix' for H3.")
                     break # Stop searching once relevant H3 is processed
            if target_ul:
                target_ul.clear(); injected_count = 0
                for bullet_text in tailored_bullets:
                    if not bullet_text: continue
                    new_li = soup.new_tag('li'); new_li.append(BeautifulSoup(bullet_text, 'html.parser')); target_ul.append(new_li); injected_count += 1
                logging.debug(f"  Injected {injected_count} tailored bullets for H3 containing '{relevant_experience_title}'."); modified = True
            elif relevant_experience_title:
                logging.warning(f"  Could not find H3 containing '{relevant_experience_title}' or its subsequent <ul>.") # Modified warning
        elif not relevant_experience_title or not tailored_bullets: logging.debug("  Skipping experience bullet injection: No relevant title or bullets provided by AI.")
        elif exp_h2: logging.debug("  Found Experience H2 but AI didn't provide title/bullets.")
        else: logging.warning("  Could not find 'Experience' heading (h2).")

        # --- 3. Rebuild Skills Section ---
        skills_h2 = soup.find(lambda t: t.name == 'h2' and 'skills' in t.get_text(strip=True).lower())
        if skills_h2 and skills_dict:
            skills_container = skills_h2.find_next_sibling('div', class_='skills-container')
            if skills_container:
                logging.debug("  Found 'div.skills-container'. Clearing/rebuilding."); skills_container.clear(); num_categories = 0
                for category, skills_data in skills_dict.items():
                     skills_list = [];
                     if isinstance(skills_data, list): skills_list = skills_data
                     elif isinstance(skills_data, str):
                         skills_list = [s.strip() for s in skills_data.split(',') if s.strip()];
                         if len(skills_list) <= 1 and skills_data: skills_list = [skills_data]; logging.warning(f"  Skills value for '{category}' was string. Processed: {skills_list}")
                     else: logging.warning(f"  Skills value for '{category}' not list/str, skipping."); continue
                     if not skills_list: logging.debug(f"  Skipping empty category '{category}'."); continue
                     num_categories += 1; col_div = soup.new_tag('div', **{'class': 'skills-column'}); col_h4 = soup.new_tag('h4'); col_h4.string = category; col_div.append(col_h4)
                     col_ul = soup.new_tag('ul', **{'class': 'skills-list'})
                     for skill_text in skills_list:
                          if not skill_text: continue
                          li = soup.new_tag('li'); li.append(BeautifulSoup(str(skill_text), 'html.parser')); col_ul.append(li)
                     col_div.append(col_ul); skills_container.append(col_div)
                logging.debug(f"  Rebuilt skills section with {num_categories} categories."); modified = True
            else: logging.warning("  Found 'Skills' H2 but no subsequent 'div.skills-container'.")
        elif skills_dict: logging.warning("  Could not find 'Skills' heading (h2).")
        else: logging.debug("  Skipping skills injection: No AI skill categories.")

        return soup.prettify(), modified
    except Exception as e: logging.error(f"Error applying AI suggestions to HTML: {e}", exc_info=True); return base_html_content, False


# CORRECTED: Refined prompt instructions for relevant_experience_title
def iterative_tailoring_and_pdf_gen(base_html_content: str, base_resume_text_for_ai: str, job_info: dict, html_filepath: Path, pdf_filepath: Path, config: dict) -> tuple[str, dict, int]:
    max_attempts = config['phase4']['max_tailoring_attempts']; status_flags = config['status']; tailoring_model = config['ai']['tailoring_model_name']; api_delay = config['ai']['api_delay_seconds']
    final_status = status_flags['FAILED_TAILORING']; latest_ai_json_data = {"error": "No successful AI tailoring attempts."}; page_count = -1
    jd_text = str(job_info.get('Job Description Plain Text', '')); original_recommendations = str(job_info.get('AI Actionable Recommendations', ''))
    is_retailoring_attempt = job_info.get('Status') == config['status']['NEEDS_RETAILORING']
    try: current_retailoring_count = int(job_info.get('Retailoring Attempts', 0))
    except (ValueError, TypeError): current_retailoring_count = 0
    if is_retailoring_attempt:
        logging.info(f"*** Re-Tailoring attempt #{current_retailoring_count + 1} ***")
        prev_summary = str(job_info.get('Generated Tailored Summary', 'N/A')); prev_bullets = str(job_info.get('Generated Tailored Bullets', 'N/A')); prev_skills = str(job_info.get('Generated Tailored Skills List', 'N/A'))
        previous_tailoring_text = f"PREVIOUS SUMMARY:\n{prev_summary}\n\nPREVIOUS BULLETS:\n{prev_bullets}\n\nPREVIOUS SKILLS:\n{prev_skills}"
    else: previous_tailoring_text = "N/A"
    for attempt in range(1, max_attempts + 1):
        logging.info(f"--- Tailoring Attempt {attempt}/{max_attempts} ---"); prompt = ""
        if attempt == 1:
            if is_retailoring_attempt:
                prompt = f"""
                **Objective:** Revise the PREVIOUSLY generated resume content... Output JSON.
                **Inputs:**
                1.  **Target Job Description:** (...)\n```{jd_text}```
                2.  **Original AI Analysis & Recommendations:** (...)\n```{original_recommendations}```
                3.  **PREVIOUS Tailoring Attempt Text:** (...)\n```{previous_tailoring_text}```
                4.  **Base Resume Text Context:** (...)\n```{base_resume_text_for_ai}```
                **Instructions:**
                1.  **Analyze:** ...
                2.  **Revise Content:** ...
                    *   `tailored_summary` (String): Rewrite summary... Use <strong> tags.
                    *   `relevant_experience_title` (String): Identify the **Company Name** heading (the exact text inside the relevant `<h3>` tag under 'Experience' in the Base Resume Text Context) that corresponds to the experience you are tailoring bullets for. For example, if tailoring bullets for Yardi, return EXACTLY "Yardi Software Pvt Ltd". **Return only the H3 text.**
                    *   `tailored_bullets` (List of Strings): Rewrite/replace previous bullets... Use <strong> tags.
                    *   `skill_categories` (Dictionary): Revise previous skills list... Use <strong> tags.
                3.  **Guidelines:** ...
                4.  **Output Format:** Respond ONLY with a single valid JSON object with keys: "tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories".
                **Generate the revised JSON object now.**
                """
            else: # Initial attempt prompt
                 prompt = f"""
                **Objective:** Generate tailored resume content... Output must be a single valid JSON object.
                **Inputs:**
                1.  **Base Resume Text Context:** (...)\n```{base_resume_text_for_ai}```
                2.  **Target Job Description:** (...)\n```{jd_text}```
                3.  **AI Analysis & Recommendations:** (...)\n```{original_recommendations}```
                **Instructions:**
                1.  **Analyze all inputs:** ...
                2.  **Generate Tailored Content:**
                    *   `tailored_summary` (String): Craft summary... Use <strong> tags.
                    *   `relevant_experience_title` (String): Identify the **Company Name** heading (the exact text inside the relevant `<h3>` tag under the 'Experience' section in the Base Resume Text Context) that corresponds to the experience you are tailoring bullets for. For example, if tailoring bullets for Yardi, return EXACTLY "Yardi Software Pvt Ltd". **Return only the H3 text.**
                    *   `tailored_bullets` (List of Strings): Rewrite/select 3-5 bullet points... Use <strong> tags.
                    *   `skill_categories` (Dictionary): Create skill dictionary... Use <strong> tags. Example: {{"Category A": ["Skill A1", "Skill A2"], "Category B": ["Skill B1"]}}
                3.  **Guidelines:** Prioritize JD keywords... Be concise...
                4.  **Output Format:** Respond ONLY with a single valid JSON object with keys: "tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories".
                **Generate the JSON object now.**
                """
        elif attempt == 2: # Condensation prompt (minor)
             if 'error' in latest_ai_json_data: logging.error("Cannot attempt condensation (Att 2) - previous AI call failed."); break
             prompt = f"""
             **Objective:** Condense the previously generated resume TEXT slightly... Output JSON.
             **Previous Generated Text Content (JSON Format):**
             ```json
             {json.dumps(latest_ai_json_data, indent=2)}
             ```
             **Instructions:** Make MINOR condensations... Preserve <strong> tags...
             Output ONLY the condensed JSON object with keys: "tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories".
             **Generate the condensed JSON object now.**
             """
        else: # Attempt 3: Condensation prompt (major)
             if 'error' in latest_ai_json_data: logging.error("Cannot attempt condensation (Att 3) - previous AI call failed."); break
             prompt = f"""
             **Objective:** Significantly shorten the previously generated resume TEXT... Output JSON.
             **Previously Condensed Text Content (JSON Format):**
             ```json
             {json.dumps(latest_ai_json_data, indent=2)}
             ```
             **Instructions:** Perform SIGNIFICANT shortening... Preserve essential <strong> tags...
             Output ONLY the significantly shortened JSON object with keys: "tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories".
             **Generate the significantly shortened JSON object now.**
             """

        gemini_response = call_gemini_api(tailoring_model, prompt, config, is_json_output=True, attempt=attempt)
        if isinstance(gemini_response, dict) and "error" in gemini_response: logging.error(f"Att {attempt}: Gemini failed: {gemini_response['error']}"); final_status = f"{status_flags['FAILED_TAILORING']} (API Err Att.{attempt})"[:250]; latest_ai_json_data = gemini_response; break
        if not isinstance(gemini_response, dict): logging.error(f"Att {attempt}: Gemini resp not dict: {type(gemini_response)}"); final_status = f"{status_flags['FAILED_TAILORING']} (Invalid Resp Att.{attempt})"; latest_ai_json_data = {"error": "Invalid API response type", "raw_response": str(gemini_response)}; break
        required_keys = ["tailored_summary", "relevant_experience_title", "tailored_bullets", "skill_categories"]
        missing_keys = [key for key in required_keys if key not in gemini_response]
        if missing_keys: logging.error(f"Att {attempt}: AI resp missing keys: {missing_keys}"); final_status = f"{status_flags['FAILED_TAILORING']} (Missing Keys Att.{attempt})"; latest_ai_json_data = {"error": f"Missing keys: {missing_keys}", "raw_response": gemini_response}; break
        latest_ai_json_data = gemini_response; logging.info(f"Attempt {attempt}: Successfully parsed AI suggestions.")
        edited_html_string, modified_flag = edit_html_with_ai_suggestions(base_html_content, latest_ai_json_data)
        try:
             with open(html_filepath, 'w', encoding='utf-8') as f: f.write(edited_html_string)
             logging.info(f"Attempt {attempt}: Saved edited HTML: {html_filepath.name}")
        except Exception as html_save_err: logging.error(f"Att {attempt}: ERROR saving HTML {html_filepath.name}: {html_save_err}"); final_status = f"{status_flags['FAILED_FILE_ACCESS']} (HTML Save Err Att.{attempt})"; break
        pdf_success = generate_pdf_from_html(html_filepath, pdf_filepath, config)
        if not pdf_success: final_status = f"{status_flags['FAILED_PDF_GEN']} (Att.{attempt})"; break
        page_count = get_pdf_page_count(pdf_filepath)
        if page_count == 1: final_status = status_flags['SUCCESS']; logging.info(f"Attempt {attempt}: SUCCESS! PDF is 1 page."); break
        elif page_count > 1: logging.warning(f"Attempt {attempt}: PDF has {page_count} pages."); final_status = status_flags['NEEDS_EDIT']
        else: final_status = f"{status_flags['FAILED_PDF_GEN']} (Validation Err Att.{attempt})"; logging.error(f"Attempt {attempt}: Error validating PDF ({page_count=})."); break
        if attempt < max_attempts: time.sleep(api_delay)
    if final_status == status_flags['NEEDS_EDIT']:
         logging.warning("Max AI condensation attempts reached, PDF still > 1 page."); logging.info("Attempting final edit: Removing last education bullet...")
         try:
              with open(html_filepath, 'r', encoding='utf-8') as f: current_html = f.read(); soup = BeautifulSoup(current_html, 'html.parser')
              edu_h2 = soup.find(lambda t: t.name == 'h2' and 'education' in t.get_text(strip=True).lower()); removed = False
              if edu_h2:
                  for edu_ul in edu_h2.find_all_next('ul', limit=5):
                       last_li = edu_ul.find_all('li', recursive=False);
                       if last_li: last_li[-1].decompose(); removed = True; logging.info(f"  Removed last li from {edu_ul.find_previous_sibling().text.strip()[:50]}..."); break # Log which section
              if removed:
                   logging.info("Removed last edu bullet. Saving & regenerating PDF...");
                   with open(html_filepath, 'w', encoding='utf-8') as f: f.write(soup.prettify())
                   if generate_pdf_from_html(html_filepath, pdf_filepath, config):
                        page_count = get_pdf_page_count(pdf_filepath)
                        if page_count == 1: final_status = status_flags['SUCCESS']; logging.info("SUCCESS: Resume now 1 page after final edit.")
                        else: logging.warning(f"WARN: Resume still {page_count} pages. Status '{status_flags['NEEDS_EDIT']}'.")
                   else: final_status = f"{status_flags['FAILED_PDF_GEN']} (Final Edit)"
              else: logging.warning("No education bullets found/removed in final edit.")
         except Exception as final_edit_err: logging.error(f"Error during final edit: {final_edit_err}"); final_status = f"{status_flags['FAILED_TAILORING']} (Final Edit Err)"
    return final_status, latest_ai_json_data, page_count

# Corrected process_resume_tailoring function
def process_resume_tailoring(config: dict, base_html_content: str, base_resume_text_for_ai: str):
    excel_filepath = config['paths']['excel_filepath']; output_folder = config['paths']['output_folder']
    status_ready = config['status']['AI_ANALYZED']; status_tailoring = config['status']['TAILORING']; status_success = config['status']['SUCCESS']
    status_needs_edit = config['status']['NEEDS_EDIT']; status_low_score = config['status']['SKIPPED_LOW_SCORE']; status_needs_retailoring = config['status']['NEEDS_RETAILORING']
    score_threshold = config['phase4']['score_threshold']; score_column_to_check = 'Total Match Score'; save_interval = config['phase4']['save_interval']
    status_flags = config['status']; retry_failed = config['workflow']['retry_failed_phase4']; max_retailoring_attempts = config['phase4']['max_retailoring_attempts']
    phase4_retry_statuses = [status_flags.get('FAILED_TAILORING'), status_flags.get('FAILED_HTML_EDIT'), status_flags.get('FAILED_PDF_GEN'), status_flags.get('FAILED_FILE_ACCESS'), status_flags.get('UNKNOWN_ERROR'), status_needs_edit, status_needs_retailoring]
    phase4_retry_statuses = [s for s in phase4_retry_statuses if s]
    logging.info(f"Starting resume tailoring process for: {excel_filepath}"); logging.info(f"Retry previously failed/needs-edit/needs-retailoring rows: {retry_failed}"); logging.info(f"Using score threshold >= {score_threshold} on column '{score_column_to_check}'")
    try:
        logging.info("Reading Excel file..."); df = pd.read_excel(excel_filepath, engine='openpyxl', dtype={'Job ID': str}); logging.info(f"Read {len(df)} rows.")
        added_cols = False
        for col in ALL_EXPECTED_COLUMNS:
            if col not in df.columns: logging.warning(f"Adding missing column '{col}'."); added_cols = True; df[col] = pd.NA if 'Score' in col or 'Attempt' in col or 'Pages' in col else ''
        if added_cols: logging.info("Reordering DataFrame columns."); df = df.reindex(columns=ALL_EXPECTED_COLUMNS)
        numeric_cols_phase4 = [col for col in ALL_EXPECTED_COLUMNS if 'Score' in col or 'Attempt' in col or 'Pages' in col]
        for col in numeric_cols_phase4: df[col] = pd.to_numeric(df[col], errors='coerce') if col in df.columns else pd.NA
        if 'Retailoring Attempts' in df.columns: df['Retailoring Attempts'] = df['Retailoring Attempts'].fillna(0).astype(int)
        else: df['Retailoring Attempts'] = 0
        text_cols_phase4 = ['Status', 'Job Description Plain Text', 'Company', 'Title', 'AI Actionable Recommendations', 'Notes', 'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List', 'Tailored HTML Path', 'Tailored PDF Path', 'Tailoring Effectiveness Status', 'AI Score Justification', 'Scraping Issues']
        for col in text_cols_phase4: df[col] = df[col].fillna('').astype(object) if col in df.columns else '' # Use object dtype here too
        if 'Job ID' not in df.columns: df['Job ID'] = ''; df['Job ID'] = df['Job ID'].fillna('').astype(str)
        else: df['Job ID'] = df['Job ID'].fillna('').astype(str)
        logging.info(f"Filtering jobs: (Status='{status_ready}' OR (Retry=True AND Status starts with retry_status)) AND {score_column_to_check} >= {score_threshold}...") # Updated log desc
        ready_mask = (df['Status'] == status_ready); score_mask = (df[score_column_to_check].fillna(-1) >= score_threshold); retry_eligible_mask = pd.Series(False, index=df.index)
        if retry_failed:
            base_error_statuses_to_retry = [ status_flags.get('FAILED_TAILORING'), status_flags.get('FAILED_HTML_EDIT'), status_flags.get('FAILED_PDF_GEN'), status_flags.get('FAILED_FILE_ACCESS'), status_flags.get('UNKNOWN_ERROR'), status_flags.get('NEEDS_EDIT'), status_flags.get('NEEDS_RETAILORING')]
            base_error_statuses_to_retry = [s for s in base_error_statuses_to_retry if s]
            for base_status in base_error_statuses_to_retry:
                retry_eligible_mask |= df['Status'].astype(str).str.startswith(base_status) # Corrected retry logic
        eligible_mask = score_mask & (ready_mask | retry_eligible_mask)
        needs_retailor_mask = df['Status'] == status_needs_retailoring; attempts_exceeded_mask = df['Retailoring Attempts'] >= max_retailoring_attempts
        eligible_mask = eligible_mask & ~(needs_retailor_mask & attempts_exceeded_mask)
        rows_to_process_idx = df[eligible_mask].index; num_to_process = len(rows_to_process_idx); logging.info(f"Found {num_to_process} jobs eligible for tailoring/re-tailoring.")
        save_needed_extra = False; low_score_mask = ready_mask & (~score_mask); max_attempts_mask = needs_retailor_mask & attempts_exceeded_mask
        if not df[low_score_mask].empty: logging.info(f"Marking {len(df[low_score_mask])} jobs as '{status_low_score}'."); df.loc[low_score_mask, 'Status'] = status_low_score; save_needed_extra = True
        if not df[max_attempts_mask].empty: logging.warning(f"Marking {len(df[max_attempts_mask])} jobs as '{status_flags['Error - Max Retailoring']}'."); df.loc[max_attempts_mask, 'Status'] = status_flags['Error - Max Retailoring']; save_needed_extra = True
        if num_to_process == 0:
            logging.info("No jobs need processing in this phase.")
            if added_cols or save_needed_extra:
                try: df.fillna('').to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Saving schema/status changes.")
                except Exception as save_err: logging.error(f"Error saving changes: {save_err}"); return False
            return True
        try: output_folder.mkdir(parents=True, exist_ok=True); logging.info(f"Output directory ready: {output_folder}")
        except Exception as e: logging.critical(f"Could not create output dir: {output_folder}. Error: {e}"); return False
        processed_in_run = 0; success_count = 0; batch_start_time = time.time()
        for index in rows_to_process_idx:
            processed_in_run += 1; job_info = df.loc[index].fillna('').to_dict(); job_title = str(job_info.get('Title', 'UnknownJob')); company_name = str(job_info.get('Company', 'UnknownCompany'))
            job_id_for_file = str(job_info.get('Job ID', f"Index{index}")); is_retailoring = job_info.get('Status') == status_needs_retailoring; log_prefix = f"Re-Tailoring Row" if is_retailoring else f"Processing Row"
            logging.info(f"--- {log_prefix} {index + 1}/{len(df)} (Index: {index}) | Job: '{job_title}' @ '{company_name}' ---")
            if is_retailoring: df.loc[index, 'Retailoring Attempts'] += 1; logging.info(f"  Re-tailoring attempt number: {df.loc[index, 'Retailoring Attempts']}")
            df.loc[index, 'Status'] = status_tailoring; job_desc_text = str(job_info.get('Job Description Plain Text', ''))
            if not job_desc_text or len(job_desc_text) < 50: logging.error(f"Skipping R{index+1}: JD text missing/short."); df.loc[index, 'Status'] = status_flags['MISSING_DATA']; continue
            company_sanitized = sanitize_filename(company_name); title_sanitized = sanitize_filename(job_title); base_filename = f"{company_sanitized}_{title_sanitized}_{job_id_for_file}"
            html_filepath = output_folder / f"{base_filename}.html"; pdf_filepath = output_folder / f"{base_filename}.pdf"
            try:
                final_status, last_ai_data, pdf_page_count = iterative_tailoring_and_pdf_gen(base_html_content, base_resume_text_for_ai, job_info, html_filepath, pdf_filepath, config)
                df.loc[index, 'Status'] = final_status; df.loc[index, 'Tailored HTML Path'] = str(html_filepath.resolve()) if html_filepath.exists() else ''; df.loc[index, 'Tailored PDF Path'] = str(pdf_filepath.resolve()) if pdf_filepath.exists() and final_status != status_flags['FAILED_PDF_GEN'] else ''
                df.loc[index, 'Tailored PDF Pages'] = pdf_page_count if pdf_page_count >= 0 else pd.NA
                if isinstance(last_ai_data, dict) and 'error' not in last_ai_data:
                    summary_raw = last_ai_data.get('tailored_summary', ''); bullets_raw = last_ai_data.get('tailored_bullets', []); skills_dict = last_ai_data.get('skill_categories', {})
                    df.loc[index, 'Generated Tailored Summary'] = strip_html_tags(summary_raw); df.loc[index, 'Generated Tailored Bullets'] = "\n".join([strip_html_tags(b) for b in bullets_raw if b])
                    skills_list_cleaned = [];
                    for cat, skills in skills_dict.items():
                        if isinstance(skills, list): skills_list_cleaned.extend([f"{cat}: {strip_html_tags(s)}" for s in skills if s])
                        elif isinstance(skills, str): skills_list_cleaned.append(f"{cat}: {strip_html_tags(skills)}")
                    df.loc[index, 'Generated Tailored Skills List'] = "\n".join(skills_list_cleaned)
                elif isinstance(last_ai_data, dict) and 'error' in last_ai_data:
                     error_info = f"AI Error: {last_ai_data['error']}" + (f" | Raw: {str(last_ai_data.get('raw_response',''))[:200]}..." if 'raw_response' in last_ai_data else "")
                     df.loc[index, 'Generated Tailored Summary'] = error_info[:1000]; df.loc[index, ['Generated Tailored Bullets', 'Generated Tailored Skills List']] = "See Summary/Notes"
                     if 'Notes' in df.columns: df.loc[index, 'Notes'] = error_info
                if final_status == status_success: success_count += 1
                logging.info(f"  Tailoring finished for row {index+1} with Status: {final_status}, PDF Pages: {pdf_page_count if pdf_page_count >=0 else 'Error'}")
            except Exception as process_err: logging.error(f"Unexpected error during tailoring loop for row {index+1}: {process_err}", exc_info=True); df.loc[index, 'Status'] = status_flags['UNKNOWN_ERROR']; df.loc[index, 'Notes'] = f"P4 Loop Err: {process_err}" if 'Notes' in df.columns else ""
            if processed_in_run % save_interval == 0:
                batch_time = time.time() - batch_start_time; logging.info(f"Processed {processed_in_run} jobs ({batch_time:.2f} sec). Saving progress...")
                try: df.fillna('').to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Progress saved."); batch_start_time = time.time()
                except PermissionError: logging.error(f"PERM ERROR saving progress: {excel_filepath}. Stopping."); return False
                except Exception as save_err: logging.error(f"Error saving progress: {save_err}"); logging.warning("Continuing...")
        logging.info("Finished Tailoring loop. Performing final save...");
        try: df_final = df.reindex(columns=ALL_EXPECTED_COLUMNS).fillna(''); df_final.to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Final Excel file saved.")
        except PermissionError: logging.error(f"FINAL SAVE ERROR: Permission denied: {excel_filepath}."); return False
        except Exception as save_err: logging.error(f"Error during final save: {save_err}"); return False
        logging.info(f"Phase 4 finished. Processed {processed_in_run} jobs. {success_count} generated as 1 page (Status='{status_success}')."); return True
    except FileNotFoundError: logging.error(f"Excel file not found: '{excel_filepath}'."); return False
    except KeyError as e: logging.error(f"Missing expected column during setup: {e}", exc_info=True); return False
    except Exception as e: logging.critical(f"Crit error during Phase 4 setup/processing: {e}", exc_info=True); return False

# --- Main Function Wrapper for Phase 4 ---
def run_phase4_resume_tailoring(config: dict) -> bool:
    """Executes Phase 4: setup, load template, process jobs (incl. re-tailoring), save."""
    logging.info("Initiating Phase 4: AI Resume Tailoring & PDF Generation")
    overall_success = False
    api_key = load_api_key(config) # Use split function
    base_html_template_content = load_base_resume_html(config) # Use split function
    if not api_key: logging.critical("API Key loading failed."); return False
    if not base_html_template_content: logging.critical("Base Resume HTML loading failed."); return False
    if not configure_gemini(api_key, config): logging.critical("Gemini config failed."); return False
    base_resume_text_for_ai = extract_text_from_html(base_html_template_content)
    if not base_resume_text_for_ai or "Error" in base_resume_text_for_ai: logging.error("Could not extract base text from HTML template."); return False
    logging.info("Successfully extracted base text from HTML template for AI prompts.")
    try:
        overall_success = process_resume_tailoring(config, base_html_template_content, base_resume_text_for_ai)
    except Exception as e:
        logging.critical(f"Unexpected critical error in run_phase4: {e}", exc_info=True)
        overall_success = False
    if overall_success: logging.info("Phase 4 processing run completed.")
    else: logging.error("Phase 4 processing run finished with critical errors or failures.")
    return overall_success

# No `if __name__ == "__main__":` block needed