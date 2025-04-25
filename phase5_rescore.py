# phase5_rescore.py
# Phase 5: Rescores tailored resumes, calculates effectiveness, and updates status.

import os
import time
import traceback
import re
import json
import logging
import random
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Union, Any # Added for type hint fix
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Attempt to import necessary libraries
try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
except ImportError:
    logging.critical("ERROR: google-generativeai library not found. Run 'pip install google-generativeai'")
    raise

# MODIFIED: Import specific required functions from Phase 3
try:
    from phase3_ai_analysis import (
        load_api_key,                   # Import NEW function
        configure_gemini,               # Keep needed helpers
        extract_text_from_html,         # Keep needed helpers
        analyze_resume_fit_with_gemini, # Keep needed helpers
        call_gemini_api,                # Keep needed helpers
        parse_safety_settings           # Keep needed helpers
    )
except ImportError as e:
     logging.critical(f"ERROR: Could not import required functions from phase3_ai_analysis.py: {e}")
     raise

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
PHASE_5_OUTPUT_COLUMNS = [ # Unchanged
    'Tailored Resume Score', 'Score Change',
    'Tailoring Effectiveness Status', 'Status'
]


# --- Helper Functions ---

# Type hint fixed based on previous feedback
def calculate_total_score(analysis_results: dict) -> Union[float, None]:
    """Calculates the total score from the component scores in the analysis results."""
    # (No changes needed from previous version)
    if not isinstance(analysis_results, dict) or analysis_results.get("error"): return pd.NA
    scores_to_sum = [analysis_results.get("Keyword Match Score", pd.NA), analysis_results.get("Achievements Score", pd.NA), analysis_results.get("Summary Quality Score", pd.NA), analysis_results.get("Tools Certs Score", pd.NA)]
    numeric_scores = [pd.to_numeric(s, errors='coerce') for s in scores_to_sum]
    valid_scores = [s for s in numeric_scores if pd.notna(s)]
    return sum(valid_scores) if valid_scores else pd.NA

# (process_rescoring function remains the same as previous version,
# including the log message for reading the *tailored* HTML file)
def process_rescoring(config: dict):
    excel_filepath = config['paths']['excel_filepath']; status_flags = config['status']
    status_ready_for_rescore = [status_flags['SUCCESS'], status_flags['NEEDS_EDIT']]; status_rescoring = status_flags['RESCORING']
    status_improved = status_flags['IMPROVED']; status_maintained = status_flags['MAINTAINED']; status_declined = status_flags['DECLINED']
    status_needs_retailoring = status_flags['NEEDS_RETAILORING']; status_error_rescoring = status_flags['FAILED_RESCORING']
    status_error_missing_html = status_flags.get('Error - Missing Tailored HTML', 'Error - Missing Tailored HTML')
    status_error_score_compare = status_flags.get('Error - Score Comparison', 'Error - Score Comparison')
    phase5_error_statuses = [status_error_rescoring, status_error_missing_html, status_error_score_compare]
    phase5_error_statuses = [s for s in phase5_error_statuses if s]
    score_threshold = config['phase4']['score_threshold']; retry_failed = config['workflow']['retry_failed_phase5']; save_interval = config['phase4'].get('save_interval', 5)
    logging.info(f"Starting tailored resume rescoring process for: {excel_filepath}"); logging.info(f"Will process jobs with status in: {status_ready_for_rescore}")
    if retry_failed: logging.info(f"Retry previously failed rows with status in: {phase5_error_statuses}")
    try:
        logging.info("Reading Excel file..."); df = pd.read_excel(excel_filepath, engine='openpyxl', dtype={'Job ID': str}); logging.info(f"Read {len(df)} rows.")
        added_cols = False
        for col in ALL_EXPECTED_COLUMNS:
            if col not in df.columns: logging.warning(f"Adding missing column '{col}'."); added_cols = True; df[col] = pd.NA if 'Score' in col or 'Attempt' in col or 'Pages' in col else ''
        if added_cols: logging.info("Reordering DataFrame columns."); df = df.reindex(columns=ALL_EXPECTED_COLUMNS)
        numeric_cols_phase5 = [col for col in ALL_EXPECTED_COLUMNS if 'Score' in col or 'Attempt' in col or 'Pages' in col]
        for col in numeric_cols_phase5: df[col] = pd.to_numeric(df[col], errors='coerce') if col in df.columns else pd.NA
        text_cols_phase5 = ['Status', 'Job Description Plain Text', 'Tailored HTML Path', 'Notes', 'Tailoring Effectiveness Status']
        for col in text_cols_phase5: df[col] = df[col].fillna('').astype(str).replace('nan', '') if col in df.columns else ''
        statuses_to_process = status_ready_for_rescore
        if retry_failed: statuses_to_process.extend(phase5_error_statuses); statuses_to_process = list(set(statuses_to_process))
        logging.info(f"Filtering for statuses: {statuses_to_process}"); rows_to_process_mask = df['Status'].isin(statuses_to_process)
        rows_to_process_idx = df[rows_to_process_mask].index; num_to_process = len(rows_to_process_idx); logging.info(f"Found {num_to_process} rows potentially needing rescoring (including retries).")
        if num_to_process == 0:
            logging.info("No rows found needing rescoring in this phase.")
            if added_cols:
                try: df.fillna('').to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Saving schema changes.")
                except Exception as save_err: logging.error(f"Error saving schema changes: {save_err}"); return False
            return True
        update_count = 0; processed_in_run = 0; batch_start_time = time.time()
        for index in rows_to_process_idx:
            processed_in_run += 1; job_title = df.loc[index, 'Title']; company_name = df.loc[index, 'Company']
            logging.info(f"--- Rescoring Row {index + 1}/{len(df)} (Index: {index}) | Job: '{job_title}' @ '{company_name}' ---"); df.loc[index, 'Status'] = status_rescoring
            tailored_html_path_str = str(df.loc[index, 'Tailored HTML Path']).strip(); jd_text = str(df.loc[index, 'Job Description Plain Text']).strip(); original_total_score = pd.to_numeric(df.loc[index, 'Total Match Score'], errors='coerce')
            tailored_html_path = None
            if tailored_html_path_str: 
                try: tailored_html_path = Path(tailored_html_path_str) 
                except Exception: logging.warning(f"Invalid HTML path: {tailored_html_path_str}"); tailored_html_path = None
            if not tailored_html_path or not tailored_html_path.is_file(): logging.error(f"Skipping R{index+1}: Tailored HTML not found: '{tailored_html_path_str}'"); df.loc[index, 'Status'] = status_error_missing_html; df.loc[index, ['Tailored Resume Score', 'Score Change']] = pd.NA; df.loc[index, 'Tailoring Effectiveness Status'] = 'Error'; continue
            if not jd_text or len(jd_text) < 50: logging.error(f"Skipping R{index+1}: Original JD text missing."); df.loc[index, 'Status'] = status_flags['MISSING_DATA']; df.loc[index, ['Tailored Resume Score', 'Score Change']] = pd.NA; df.loc[index, 'Tailoring Effectiveness Status'] = 'Error'; continue
            if pd.isna(original_total_score): logging.warning(f"Skipping R{index+1}: Original Total Score missing."); df.loc[index, 'Status'] = status_error_score_compare; df.loc[index, ['Tailored Resume Score', 'Score Change']] = pd.NA; df.loc[index, 'Tailoring Effectiveness Status'] = 'Error - Orig Score NA'; continue
            try:
                logging.info(f"  Reading tailored HTML file: {tailored_html_path.name}") # Log correct file
                with open(tailored_html_path, 'r', encoding='utf-8') as f: tailored_html_content = f.read()
                tailored_resume_text = extract_text_from_html(tailored_html_content)
                if not tailored_resume_text or "Error" in tailored_resume_text: raise ValueError("Failed text extraction from tailored HTML.")
            except Exception as read_err: logging.error(f"Skipping R{index+1}: Failed read/extract from tailored HTML '{tailored_html_path.name}': {read_err}"); df.loc[index, 'Status'] = status_error_missing_html; df.loc[index, ['Tailored Resume Score', 'Score Change']] = pd.NA; df.loc[index, 'Tailoring Effectiveness Status'] = 'Error - HTML Read Failed'; continue
            logging.info("  Calling AI to analyze tailored resume text against JD..."); analysis_results = analyze_resume_fit_with_gemini(tailored_resume_text, jd_text, config); time.sleep(config['ai']['api_delay_seconds'])
            if analysis_results.get("error"):
                 logging.error(f"  Rescoring AI analysis failed: {analysis_results['error']}"); df.loc[index, 'Status'] = status_error_rescoring; df.loc[index, ['Tailored Resume Score', 'Score Change']] = pd.NA; df.loc[index, 'Tailoring Effectiveness Status'] = 'Error - AI Rescore Failed'
                 if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"P5 Rescore Err: {analysis_results['error']}"
            else:
                 tailored_total_score = calculate_total_score(analysis_results); df.loc[index, 'Tailored Resume Score'] = tailored_total_score; logging.info(f"  Tailored Resume Score (Total): {tailored_total_score}")
                 if pd.notna(tailored_total_score) and pd.notna(original_total_score):
                     score_change = tailored_total_score - original_total_score; df.loc[index, 'Score Change'] = score_change; logging.info(f"  Score Change: {score_change:+.2f} (Original: {original_total_score}, Tailored: {tailored_total_score})")
                     effectiveness_status = status_declined
                     if tailored_total_score >= score_threshold: effectiveness_status = status_improved if score_change > 0 else status_maintained
                     else: effectiveness_status = status_needs_retailoring
                     df.loc[index, 'Tailoring Effectiveness Status'] = effectiveness_status; df.loc[index, 'Status'] = effectiveness_status; logging.info(f"  Effectiveness Status: {effectiveness_status}"); update_count += 1
                 else: logging.error("  Could not calculate score change."); df.loc[index, 'Score Change'] = pd.NA; df.loc[index, 'Status'] = status_error_score_compare; df.loc[index, 'Tailoring Effectiveness Status'] = 'Error - Score Calc Failed'
            if processed_in_run % save_interval == 0:
                batch_time = time.time() - batch_start_time; logging.info(f"Processed {processed_in_run} rows ({batch_time:.2f} sec). Saving progress...")
                try: df.fillna('').to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Progress saved."); batch_start_time = time.time()
                except PermissionError: logging.error(f"PERM ERROR saving progress: {excel_filepath}. Stopping."); return False
                except Exception as save_err: logging.error(f"Error saving progress: {save_err}"); logging.warning("Continuing...")
        logging.info("Finished Rescoring loop. Performing final save...");
        try: df_final = df.reindex(columns=ALL_EXPECTED_COLUMNS).fillna(''); df_final.to_excel(excel_filepath, index=False, engine='openpyxl'); logging.info("Final Excel file saved.")
        except PermissionError: logging.error(f"FINAL SAVE ERROR: Permission denied: {excel_filepath}."); return False
        except Exception as save_err: logging.error(f"Error during final save: {save_err}"); return False
        logging.info(f"Phase 5 finished. Successfully rescored/evaluated {update_count} out of {num_to_process} targeted rows."); return True
    except FileNotFoundError: logging.error(f"Excel file not found: '{excel_filepath}'."); return False
    except KeyError as e: logging.error(f"Missing expected column during setup: {e}", exc_info=True); return False
    except Exception as e: logging.critical(f"Crit error during Phase 5 setup/processing: {e}", exc_info=True); return False


# --- Main Function Wrapper for Phase 5 ---
# MODIFIED: Uses only load_api_key
def run_phase5_rescoring(config: dict) -> bool:
    """Executes Phase 5: Load API key, configure Gemini, process Excel for rescoring, save."""
    logging.info("Initiating Phase 5: Tailored Resume Rescoring")
    overall_success = False

    # Use new split function to load only the API key
    api_key = load_api_key(config)
    if not api_key:
        logging.critical("API Key loading failed. Phase 5 cannot proceed.")
        return False

    # Configure Gemini (needs API key)
    if not configure_gemini(api_key, config):
        logging.critical("Gemini API configuration failed. Phase 5 cannot proceed.")
        return False

    # --- Process Rescoring ---
    try:
        overall_success = process_rescoring(config)
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in run_phase5: {e}", exc_info=True)
        overall_success = False

    if overall_success:
        logging.info("Phase 5 processing run completed.")
    else:
        logging.error("Phase 5 processing run finished with critical errors or failures.")

    return overall_success

# No `if __name__ == "__main__":` block needed