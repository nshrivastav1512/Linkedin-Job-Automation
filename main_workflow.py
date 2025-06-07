# main_workflow.py
# Phase 0: Configuration, Logging Setup, and Workflow Orchestration

import os
import sys
import time
import traceback
from pathlib import Path
from datetime import datetime
import logging
import json # For loading JSON-like config parts if needed later

# --- Add Project Directory to Python Path ---
# Ensures that phase modules can be imported correctly
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
print(f"[Startup] Added project directory to sys.path: {BASE_DIR}")

# --- Attempt to Import Phase Logic ---
# Wrap in try-except to give immediate feedback if files are missing
try:
    import phase1_list_scraper
    import phase2_detail_scraper
    import phase3_ai_analysis
    import phase4_tailoring
    # Add import for the new Phase 5
    import phase5_rescore
    print("[Startup] Successfully imported phase modules (1-5).")
except ImportError as e:
    print(f"!!!!!! ERROR: Failed to import one or more phase scripts: {e} !!!!!")
    print("Ensure the following files exist in the same directory as main_workflow.py:")
    print(" - phase1_list_scraper.py")
    print(" - phase2_detail_scraper.py")
    print(" - phase3_ai_analysis.py")
    print(" - phase4_tailoring.py")
    # Add Phase 5 to the list
    print(" - phase5_rescore.py")
    print("And that they contain the required run_phaseX_... functions.")
    sys.exit(1) # Exit if essential imports fail

# ==============================================================================
# --- Configuration ---
# Centralized configuration for the entire workflow.
# ==============================================================================
print("[Config] Loading configuration settings...")

# --- 1. File Paths & Core Settings ---
CONFIG_PATHS = {
    "base_dir": BASE_DIR,
    # MANDATORY: The central Excel file used throughout the process.
    "excel_filepath": BASE_DIR / "linkedin_jobs_master_list.xlsx",
    # MANDATORY: Your base resume in HTML format (template for Phase 4).
    "resume_filepath_html": BASE_DIR / "Resume.html",
    # MANDATORY: Folder where tailored resumes (HTML/PDF) will be saved.
    "output_folder": BASE_DIR / "Tailored_Resumes",
    # MANDATORY: Folder where log files will be stored.
    "log_folder": BASE_DIR / "logs",
    # MANDATORY: Path to your .env file containing API keys (e.g., GEMINI_API_KEY).
    "env_filepath": BASE_DIR / '.env',
}

# --- 2. Selenium Configuration ---
#  ---- Add Chrome Debugger Instructions ---
# (Instructions remain the same)
# --- HOW TO START CHROME WITH REMOTE DEBUGGING ---
# Before running this script, you MUST start Chrome manually using the command line
# and specify the same debugging port as configured below (e.g., 9222).
# Ensure ALL other Chrome instances are closed first.
#
# Windows (Command Prompt - Adjust path if necessary):
# "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\ChromeDebugProfile"
# (Using a separate user-data-dir is recommended to avoid conflicts with your main profile)
#
# macOS (Terminal):
# /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --user-data-dir="$HOME/ChromeDebugProfile"
#
# Linux (Terminal - Adjust command if needed):
# google-chrome --remote-debugging-port=9222 --user-data-dir="$HOME/ChromeDebugProfile"
#
# After starting Chrome this way, navigate manually to linkedin.com and log in if needed.
# THEN run this Python script.
# ---------------------------------------------------
CONFIG_SELENIUM = {
    # MANDATORY: Full path to your ChromeDriver executable.
    "chromedriver_path": r"C:\AI Use and Deveopment\chromedriver-win64\chromedriver.exe", # Use raw string (r"...")
    # MANDATORY: Port for the Chrome debugger interface (ensure Chrome is started with --remote-debugging-port=XXXX).
    "debugger_port": 9222,
    # OPTIONAL: Wait time in seconds for elements to appear (shorter waits).
    "wait_time_short": 15,
    # MODIFIED: Wait time in seconds for elements to appear (longer waits, e.g., page loads). Default reduced.
    "wait_time_long": 20, # Reduced from 30 per Proposal #7
    # OPTIONAL: Add random delays between certain actions to mimic human behavior.
    "enable_random_delays": True,
    "delay_short_base": 1.5, # Base seconds for short delays
    "delay_short_variance": 1.0, # Max additional random seconds for short delays
    "delay_medium_base": 3.0, # Base seconds for medium delays
    "delay_medium_variance": 2.0, # Max additional random seconds for medium delays
    "delay_long_base": 5.0, # Base seconds for long delays (e.g., page load)
    "delay_long_variance": 3.0, # Max additional random seconds for long delays
}

# --- 3. Phase 1: Job List Scraping Configuration ---
CONFIG_PHASE1 = {
    # MANDATORY: The job title or keywords to search for.
    "search_term": "SQL Support",
    # OPTIONAL: The location to search within (e.g., "City, State, Country" or "Country"). Leave empty ('') if not needed.
    "search_location_text": "Pune, Maharashtra, India",
    # OPTIONAL: LinkedIn's internal Geo ID for the location (more precise). Find using network tools or online resources. Leave empty ('') if using location_text only or no location.
    "search_geo_id": "",#114806696
    # MANDATORY: Date filter choice. '1': Any time, '2': Past month, '3': Past week, '4': Past 24 hours.
    "date_filter_choice": "4", # Defaulting to 'Past 24 hours'
    # OPTIONAL: Set to True to scrape multiple pages (up to max_pages). False scrapes only the first page.
    "scrape_all_pages": True, # Changed default to True as it's more common
    # OPTIONAL: Maximum number of pages to scrape if scrape_all_pages is True. LinkedIn typically limits to 40.
    "max_pages_to_scrape": 2, # Reduced default for faster testing
    # OPTIONAL: Save the Excel file after each page is scraped (more robust but slower).
    "save_after_each_page": False,
    # OPTIONAL: Set to True for detailed console output during card extraction (can be noisy).
    "verbose_card_extraction": False,
    # OPTIONAL: Limit the number of *successfully* scraped jobs per page. Set to 0 or None for no limit per page.
    "jobs_per_page_limit": 0,
    # OPTIONAL: Limit the total number of *successfully* scraped jobs for the entire Phase 1 run. Set to 0 or None for no total limit.
    "total_jobs_limit": 10, # Example: Set to 100 to stop after 100 jobs are scraped
    # NEW (Proposal #6): Minimum number of NEW UNIQUE jobs to add in Phase 1 before stopping (unless max_pages hit).
    "minimum_unique_jobs_target": 10, # Set to 0 or None to disable this minimum target.
}

# --- 4. Phase 2: Job Detail Scraping Configuration ---
CONFIG_PHASE2 = {
    # OPTIONAL: Save progress to Excel every N jobs processed. Helps resume if interrupted.
    "save_interval": 5,
}

# --- 5. Phase 3 & 4 & 5: AI (Gemini) Configuration ---
# Note: Phase 5 reuses analysis model/config
CONFIG_AI = {
    # MANDATORY: The name of the API key variable in your .env file.
    "api_key_name": "GEMINI_API_KEY",
    # MANDATORY: Model for extracting structured data (responsibilities, skills) from JD. Flash is faster/cheaper.
    "extraction_model_name": "gemini-2.0-flash", # Using pro for potentially better extraction
    # MANDATORY: Model for the detailed resume vs. JD analysis and scoring (used in Phase 3 and Phase 5). Pro often yields better results here.
    "analysis_model_name": "gemini-2.0-flash", # Using flash for speed in analysis/rescoring
    # MANDATORY: Model for generating the tailored resume content (summary, bullets, skills) in Phase 4. Pro is recommended for quality.
    "tailoring_model_name": "gemini-2.0-flash", # Using pro for tailoring quality
    # OPTIONAL: Delay (in seconds) between consecutive Gemini API calls to help avoid rate limits.
    "api_delay_seconds": 5, # Slightly reduced, monitor rate limits
    # OPTIONAL: Safety settings for Gemini API calls. Review Google's policy.
    "safety_settings": {
        "HARM_CATEGORY_HARASSMENT": "BLOCK_NONE", # Adjust as needed, BLOCK_NONE is permissive
        "HARM_CATEGORY_HATE_SPEECH": "BLOCK_NONE",
        "HARM_CATEGORY_SEXUALLY_EXPLICIT": "BLOCK_NONE",
        "HARM_CATEGORY_DANGEROUS_CONTENT": "BLOCK_NONE",
    },
    # OPTIONAL: Generation config for API calls expecting JSON output (extraction, tailoring).
    "generation_config_json": {
        "temperature": 0.6, # Slightly lower temp for more deterministic JSON structure
        "top_p": 1,
        "top_k": 1,
        "max_output_tokens": 8192, # Increased max tokens
        "response_mime_type": "application/json",
    },
    # OPTIONAL: Generation config for API calls expecting Text output (analysis in P3 & P5).
    "generation_config_text": {
        "temperature": 0.7, # Moderate temp for creative but focused text
        "top_p": 1,
        "top_k": 1,
        "max_output_tokens": 8192, # Increased max tokens
        # "response_mime_type": "text/plain", # Default, usually not needed
    },
    # Path to resume HTML (used for text extraction input to AI)
    "resume_html_filepath": CONFIG_PATHS["resume_filepath_html"], # Reference central path config
}

# --- 6. Phase 4 & 5: Tailoring Configuration ---
CONFIG_PHASE4 = {
    # MANDATORY: Minimum AI Match Score (Total Match Score) required to trigger tailoring. Adjust based on desired selectivity.
    "score_threshold": 2.5, # Using Total Match Score from P3 analysis
    # OPTIONAL: Maximum number of attempts the AI will make to tailor and fit the resume onto one page.
    "max_tailoring_attempts": 3,
    # NEW (Proposal #10): Max re-tailoring attempts triggered by Phase 5.
    "max_retailoring_attempts": 2, # Set to 0 to disable re-tailoring loop
    # OPTIONAL: Save progress to Excel every N resumes tailored.
    "save_interval": 3,
    # Path to the base HTML resume template file (used as input for tailoring).
    "html_template_filepath": CONFIG_PATHS["resume_filepath_html"], # Reference central path config
}

# --- 7. Status Flags ---
# UPDATED with Phase 5 statuses
CONFIG_STATUS = {
    # Phase 1 & 2 Initials
    "NEW": "New",
    "PROCESSING_DETAILS": "Processing Details",
    "READY_FOR_AI": "Ready for AI",
    # Phase 3 Statuses
    "PROCESSING_AI": "Processing AI Analysis",
    "AI_ANALYZED": "AI Analyzed",
    # Phase 4 Statuses
    "SKIPPED_LOW_SCORE": "Skipped - Low AI Score",
    "TAILORING": "Tailoring Resume",
    "SUCCESS": "Tailored Resume Created", # PDF generation successful (might be >1 page initially)
    "NEEDS_EDIT": "Tailored Needs Manual Edit", # PDF > 1 page after AI attempts
    # Phase 5 Statuses (NEW)
    "RESCORING": "Rescoring Tailored Resume", # Phase 5 working on this
    "IMPROVED": "Rescored - Improved", # Score increased and >= threshold
    "MAINTAINED": "Rescored - Maintained", # Score >= threshold, but didn't increase (or decreased slightly but still meets threshold)
    "DECLINED": "Rescored - Declined", # Score decreased significantly or below threshold but not triggering re-tailor yet
    "NEEDS_RETAILORING": "Needs Re-Tailoring", # Score below threshold after tailoring, triggers Phase 4 retry
    # Shared Error Statuses
    "FAILED_SCRAPE_LIST": "Error - Scrape Job List",
    "FAILED_SCRAPE_DETAILS": "Error - Scrape Job Details",
    "FAILED_AI_EXTRACTION": "Error - AI Extraction",
    "FAILED_AI_ANALYSIS": "Error - AI Analysis",
    "FAILED_TAILORING": "Error - AI Tailoring",
    "FAILED_HTML_EDIT": "Error - HTML Edit",
    "FAILED_PDF_GEN": "Error - PDF Generation",
    "FAILED_RESCORING": "Error - Rescoring Failed", # New Phase 5 Error
    "FAILED_FILE_ACCESS": "Error - File Access",
    "FAILED_API_CONFIG": "Error - API Config/Auth",
    "FAILED_WEBDRIVER": "Error - WebDriver Connection",
    "INVALID_LINK": "Error - Invalid Job Link",
    "MISSING_DATA": "Error - Missing Input Data",
    "Error - Max Retailoring": "Error - Max Re-Tailoring Attempts", # New status if re-tailoring limit hit
    "Error - Score Comparison": "Error - Score Comparison Failed", # Error during P5 logic
    "Error - Missing Tailored HTML": "Error - Tailored HTML Missing for Rescore", # Error during P5 logic
    "UNKNOWN_ERROR": "Error - Unknown",
}


# --- 8. LinkedIn Selectors ---
# Keep these centralized. Update if LinkedIn changes its HTML structure.
# Last Updated: 2025-04-17 (Based on user-provided HTML) - No changes proposed here.
CONFIG_LINKEDIN_SELECTORS = {
    # --- Phase 1: Job List Page ---
    "job_list_container": "div.scaffold-layout__list",
    "job_card": "li.scaffold-layout__list-item[data-occludable-job-id]",
    "job_card_link": "a.job-card-list__title, a.job-card-container__link, a.base-card__full-link",
    "job_card_title": "strong",
    "job_card_company": "div.artdeco-entity-lockup__subtitle span",
    "job_card_location": "ul.job-card-container__metadata-wrapper li:first-child span",
    #"job_card_logo": "img.artdeco-entity-image", # REMOVED per Proposal #11
    "job_card_footer_list": "ul.job-card-list__footer-wrapper",
    "job_card_posted_time": "time",
    "job_card_salary": "li.job-card-container__metadata-item--salary, div[class*='salary']",
    "job_card_insights": ".job-card-list__insight .job-card-container__job-insight-text",
    "job_card_verified_icon": 'svg[data-test-icon="verified-small"]',
    "pagination_container": "ul.artdeco-pagination__pages",
    "pagination_button_template": "button[aria-label='Page {}']",
    "no_results_banner": "//h1[contains(text(),'No matching jobs found')]",

    # --- Phase 2: Job Detail Page ---
    "details_main_container": "div.job-view-layout.jobs-details",
    "details_top_card": "div.p5", # Targeting the div with padding containing core elements
    "details_company_link": ".job-details-jobs-unified-top-card__company-name a",
    "details_metadata_container": ".job-details-jobs-unified-top-card__primary-description-container",
    "details_posted_ago_fallback": "span.jobs-unified-top-card__posted-date",
    "details_easy_apply_button": "button.jobs-apply-button[aria-label*='Easy Apply']",
    "details_description_container": "div#job-details",
    "details_show_more_button": "button.jobs-description__footer-button",
    "details_company_section": "section.jobs-company",
    "details_company_followers_subtitle": "div.artdeco-entity-lockup__subtitle",
    "details_company_info_div": "div.t-14.mt5",
    "details_company_about_text": "p.jobs-company__company-description",
    "details_company_show_more_button": "button.inline-show-more-text__button",
    "details_hiring_team_section_xpath": "//h2[normalize-space()='Meet the hiring team']/following-sibling::div",
    "details_hiring_team_card": "div.display-flex.align-items-center.mt4",
    "details_hiring_team_name": "span.jobs-poster__name strong",
    "details_hiring_team_profile_link": "a[href*='/in/']",
    # Member 2 selectors are no longer needed per Proposal #11
}

# --- 9. Workflow Control ---
CONFIG_WORKFLOW = {
    # MANDATORY: First phase to execute (1, 2, 3, 4, or 5). Set > 1 to skip earlier phases.
    "start_phase": 3,
    # MANDATORY: Last phase to execute (1, 2, 3, 4, or 5).
    "end_phase": 5, # Defaulting to include Phase 5
    # OPTIONAL: Set True to retry processing rows that previously failed in Phase 2.
    "retry_failed_phase2": True,
    # OPTIONAL: Set True to retry processing rows that previously failed in Phase 3.
    "retry_failed_phase3": True,
    # OPTIONAL: Set True to retry processing rows that previously failed/needs-edit in Phase 4 (Tailoring/PDF).
    "retry_failed_phase4": True,
    # OPTIONAL: Set True to retry processing rows that failed in Phase 5 (Rescoring).
    "retry_failed_phase5": True, # Added option for Phase 5 retries
}

# ==============================================================================
# --- Master Configuration Dictionary ---
# Combine all configuration pieces into a single dictionary for easy passing.
# ==============================================================================
MASTER_CONFIG = {
    "paths": CONFIG_PATHS,
    "selenium": CONFIG_SELENIUM,
    "workflow": CONFIG_WORKFLOW,
    "phase1": CONFIG_PHASE1,
    "phase2": CONFIG_PHASE2,
    "ai": CONFIG_AI,
    "phase4": CONFIG_PHASE4, # Phase 4 config also used by Phase 5 implicitly (threshold)
    "status": CONFIG_STATUS,
    "selectors": CONFIG_LINKEDIN_SELECTORS,
}

# ==============================================================================
# --- Logging Setup ---
# ==============================================================================
def setup_logging(config):
    """Configures logging to console and a dated file."""
    log_folder = config['paths']['log_folder']
    # Handle potential missing keys during startup logging
    search_term = config['phase1'].get('search_term', 'UnknownSearch')
    location_text = config['phase1'].get('search_location_text', 'UnknownLocation')
    search_term_safe = "".join(c if c.isalnum() else "_" for c in search_term)
    location_safe = "".join(c if c.isalnum() else "_" for c in location_text)[:20]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"log_{timestamp}_{search_term_safe}_{location_safe}.log"
    log_filepath = log_folder / log_filename

    try:
        log_folder.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"!!!!!! ERROR: Could not create log directory: {log_folder}. Error: {e} !!!!!")
        print("Logging to console only.")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)-8s - [%(filename)s:%(lineno)d] - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        return

    # Configure logging
    logging.basicConfig(
        level=logging.INFO, # Set base level to INFO
        format='%(asctime)s - %(levelname)-8s - [%(filename)s:%(lineno)d] - %(message)s', # Simplified format slightly
        handlers=[
            logging.FileHandler(log_filepath, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # Silence noisy libraries
    logging.getLogger("weasyprint").setLevel(logging.ERROR)
    logging.getLogger("fontTools").setLevel(logging.WARNING)
    logging.getLogger("woff2").setLevel(logging.WARNING)
    # Optional: Reduce Selenium/Urllib3 noise further if needed
    # logging.getLogger("selenium.webdriver.remote.remote_connection").setLevel(logging.WARNING)
    # logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)


    logging.info("=================================================")
    logging.info(f"Logging initialized. Log file: {log_filepath}")
    logging.info("Starting Job Automation Workflow")
    logging.info(f"Base Directory: {config['paths']['base_dir']}")
    logging.info(f"Using Excel File: {config['paths']['excel_filepath']}")
    logging.info(f"Search Term: '{config['phase1'].get('search_term', 'N/A')}'")
    logging.info(f"Location: '{config['phase1'].get('search_location_text', 'N/A')}' (GeoID: {config['phase1'].get('search_geo_id', 'N/A')})")
    logging.info(f"Workflow Phases: {config['workflow']['start_phase']} to {config['workflow']['end_phase']}")
    logging.info("=================================================")

# ==============================================================================
# --- Excel File Accessibility Check (Proposal #1) ---
# ==============================================================================
def check_excel_accessibility(filepath: Path):
    """Checks if the Excel file can be opened for writing, prompts user if locked."""
    logging.info(f"Checking accessibility of Excel file: {filepath}")
    retry_delay = 5 # seconds to wait between retries after user prompt
    while True:
        try:
            # Attempt to open in append mode, which requires write access.
            # Using 'a+' also allows reading if needed later, but 'a' is sufficient for check.
            with open(filepath, 'a'):
                pass # Successfully opened and closed
            logging.info(f"Excel file '{filepath.name}' is accessible.")
            return True # File is accessible
        except PermissionError:
            logging.error(f"PERMISSION ERROR: Cannot access Excel file: {filepath}")
            logging.error("The file might be open in Excel or another application.")
            logging.warning("Please close the file to allow the script to continue.")
            user_input = input(f"Press Enter to retry access after closing the file, or type 'exit' to quit: ").strip().lower()
            if user_input == 'exit':
                logging.info("User chose to exit due to file access issue.")
                return False # User chose to exit
            else:
                logging.info(f"Retrying file access in {retry_delay} seconds...")
                time.sleep(retry_delay)
                # Loop continues
        except FileNotFoundError:
            logging.warning(f"Excel file '{filepath.name}' not found. It will be created by Phase 1 if run.")
            # Consider this accessible for now, Phase 1 will handle creation.
            return True
        except Exception as e:
            logging.critical(f"Unexpected error checking Excel file accessibility: {e}", exc_info=True)
            return False # Critical unexpected error

# ==============================================================================
# --- Main Workflow Orchestration ---
# UPDATED to include Phase 5
# ==============================================================================
def run_workflow(config):
    """Runs the phases sequentially, respecting start/end phase config and Phase 5 loop."""
    logging.info("########## Starting Workflow ##########")
    start_phase = config['workflow']['start_phase']
    end_phase = config['workflow']['end_phase']
    max_phase = 5 # Current maximum phase number
    logging.info(f"Workflow configured to run from Phase {start_phase} to Phase {end_phase}.")

    if start_phase < 1 or end_phase > max_phase or start_phase > end_phase:
        logging.error(f"Invalid phase range ({start_phase}-{end_phase}). Must be between 1 and {max_phase}.")
        return False, {}

    overall_success = True # Assume success unless a critical error occurs
    phase_times = {}
    phases_to_run = range(start_phase, end_phase + 1)

    # --- Phase 1: Scrape Job List ---
    if 1 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 1: Scrape Job List ---")
        try:
            # Assuming Phase 1 function now returns (success_bool, total_added, total_skipped)
            success_phase1, added_p1, skipped_p1 = phase1_list_scraper.run_phase1_job_list_scraping(config)
            if not success_phase1:
                logging.error("Phase 1 failed critically. Aborting workflow.")
                overall_success = False
            else:
                logging.info(f"--- Phase 1 Completed (Added: {added_p1}, Skipped Duplicates: {skipped_p1}) ---")
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 1 execution: {e}", exc_info=True)
            overall_success = False
        phase_times['Phase 1'] = time.time() - phase_start_time
        logging.info(f"Phase 1 duration: {phase_times.get('Phase 1', 0):.2f} seconds.")
    elif start_phase > 1: # Only log skip if it wasn't the first phase intended
        logging.info("--- Skipping Phase 1 (Based on start_phase config) ---")

    # --- Phase 2: Scrape Job Details ---
    if overall_success and 2 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 2: Scrape Job Details ---")
        try:
            success_phase2 = phase2_detail_scraper.run_phase2_detail_scraping(config)
            if not success_phase2:
                logging.warning("Phase 2 encountered critical errors (check logs). Proceeding cautiously.")
                # Decide if Phase 2 failure halts the whole workflow
                # overall_success = False # Keep commented unless P2 failure MUST stop everything
            else:
                logging.info("--- Phase 2 Completed ---")
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 2 execution: {e}", exc_info=True)
            overall_success = False
        phase_times['Phase 2'] = time.time() - phase_start_time
        logging.info(f"Phase 2 duration: {phase_times.get('Phase 2', 0):.2f} seconds.")
    elif 2 in phases_to_run: # Log skip only if it was supposed to run but previous failed
         logging.warning("--- Skipping Phase 2 due to previous critical failure ---")
    elif start_phase > 2:
         logging.info("--- Skipping Phase 2 (Based on start_phase config) ---")

    # --- Phase 3: AI Analysis & Scoring ---
    if overall_success and 3 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 3: AI Analysis & Scoring ---")
        try:
            success_phase3 = phase3_ai_analysis.run_phase3_ai_processing(config)
            if not success_phase3:
                logging.warning("Phase 3 encountered critical errors (check logs). Proceeding cautiously.")
                # overall_success = False # Keep commented unless P3 must fully succeed
            else:
                 logging.info("--- Phase 3 Completed ---")
        except ImportError as e: # Catch specific library import errors
             if 'google.generativeai' in str(e):
                 logging.critical("CRITICAL ERROR: google-generativeai library not installed. Run 'pip install google-generativeai'.")
             else:
                  logging.critical(f"CRITICAL UNHANDLED ImportError during Phase 3: {e}", exc_info=True)
             overall_success = False
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 3 execution: {e}", exc_info=True)
            overall_success = False
        phase_times['Phase 3'] = time.time() - phase_start_time
        logging.info(f"Phase 3 duration: {phase_times.get('Phase 3', 0):.2f} seconds.")
    elif 3 in phases_to_run:
         logging.warning("--- Skipping Phase 3 due to previous critical failure ---")
    elif start_phase > 3:
         logging.info("--- Skipping Phase 3 (Based on start_phase config) ---")

    # --- Phase 4: AI Tailoring & PDF Generation ---
    # Phase 4 might now be re-run if Phase 5 flags jobs
    # The logic here assumes Phase 4 handles the 'NEEDS_RETAILORING' status internally if run
    if overall_success and 4 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 4: AI Resume Tailoring & PDF Generation ---")
        try:
            success_phase4 = phase4_tailoring.run_phase4_resume_tailoring(config)
            if not success_phase4:
                logging.warning("Phase 4 encountered critical errors (check logs and 'Tailored_Resumes' folder).")
                # Phase 4 handles many row-level errors, maybe don't set overall_success=False unless severe?
                # overall_success = False # Failure here might prevent Phase 5
            else:
                 logging.info("--- Phase 4 Completed ---")
        except ImportError as e: # Catch specific library import errors
             if 'weasyprint' in str(e):
                 logging.critical("CRITICAL ERROR: WeasyPrint library not installed or missing system dependencies (GTK+). See WeasyPrint documentation.")
             elif 'PyPDF2' in str(e):
                 logging.critical("CRITICAL ERROR: PyPDF2 library not installed. Run 'pip install pypdf2'.")
             else:
                  logging.critical(f"CRITICAL UNHANDLED ImportError during Phase 4: {e}", exc_info=True)
             overall_success = False
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 4 execution: {e}", exc_info=True)
            overall_success = False
        phase_times['Phase 4'] = time.time() - phase_start_time
        logging.info(f"Phase 4 duration: {phase_times.get('Phase 4', 0):.2f} seconds.")
    elif 4 in phases_to_run:
         logging.warning("--- Skipping Phase 4 due to previous critical failure ---")
    elif start_phase > 4:
         logging.info("--- Skipping Phase 4 (Based on start_phase config) ---")


    # --- Phase 5: Rescore Tailored Resumes ---
    # Run Phase 5 only if it's in the configured range AND previous phases didn't critically fail
    if overall_success and 5 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 5: Rescore Tailored Resumes & Check Effectiveness ---")
        try:
            success_phase5 = phase5_rescore.run_phase5_rescoring(config)
            if not success_phase5:
                logging.warning("Phase 5 encountered critical errors (check logs).")
                # Decide if Phase 5 failure is critical overall
                # overall_success = False
            else:
                 logging.info("--- Phase 5 Completed ---")
                 # Check if any jobs were marked for re-tailoring
                 # This requires phase5 to potentially signal this back or check Excel status.
                 # For now, assume user might re-run phases 4-5 if needed.
                 # TODO (Optional): Add logic here to check Excel for 'NEEDS_RETAILORING' status
                 # and potentially loop back to Phase 4 automatically (complex).
                 # Simple approach: Log if re-tailoring is needed.
                 # df_check = pd.read_excel(config['paths']['excel_filepath'], engine='openpyxl')
                 # needs_retailor_count = df_check[df_check['Status'] == config['status']['NEEDS_RETAILORING']].shape[0]
                 # if needs_retailor_count > 0:
                 #    logging.warning(f"{needs_retailor_count} job(s) marked as 'Needs Re-Tailoring'. Consider re-running Phase 4 and 5.")

        except ImportError as e:
             # Catch specific library import errors if Phase 5 has unique ones
             logging.critical(f"CRITICAL UNHANDLED ImportError during Phase 5: {e}", exc_info=True)
             overall_success = False
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 5 execution: {e}", exc_info=True)
            overall_success = False
        phase_times['Phase 5'] = time.time() - phase_start_time
        logging.info(f"Phase 5 duration: {phase_times.get('Phase 5', 0):.2f} seconds.")
    elif 5 in phases_to_run:
         logging.warning("--- Skipping Phase 5 due to previous critical failure ---")
    elif start_phase > 5:
         logging.info("--- Skipping Phase 5 (Based on start_phase config) ---")


    # --- Workflow End ---
    logging.info("#################################################")
    if overall_success:
        logging.info(f"Job Automation Workflow Completed (Phases {start_phase}-{end_phase}).")
    else:
        logging.error("Job Automation Workflow Halted or Completed with CRITICAL ERRORS.")
    logging.info("Review log file for detailed information.")
    logging.info("#################################################")
    return overall_success, phase_times

# ==============================================================================
# --- Script Execution ---
# ==============================================================================
if __name__ == "__main__":
    global_start_time = time.time()

    # Load environment variables (like API keys) from .env file
    print(f"[Startup] Loading environment variables from: {CONFIG_PATHS['env_filepath']}...")
    if CONFIG_PATHS['env_filepath'].exists():
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=CONFIG_PATHS['env_filepath'])
        print("[Startup] Environment variables loaded.")
    else:
        print(f"[Startup] WARNING: .env file not found at {CONFIG_PATHS['env_filepath']}. API keys may be missing.")

    # Setup logging *after* loading config
    setup_logging(MASTER_CONFIG)

    # --- NEW: Pre-run check for Excel file accessibility (Proposal #1) ---
    excel_file_path = MASTER_CONFIG['paths']['excel_filepath']
    if not check_excel_accessibility(excel_file_path):
        logging.critical("Exiting script because Excel file is inaccessible or user chose to exit.")
        sys.exit(1)
    # --- End Excel Check ---

    # Run the main workflow
    workflow_status, phase_durations = run_workflow(MASTER_CONFIG)

    global_end_time = time.time()
    total_runtime = global_end_time - global_start_time
    logging.info(f"Total Workflow Runtime: {total_runtime:.2f} seconds.")
    logging.info(f"Phase Durations (seconds): {phase_durations}") # Added units
    logging.info("Script execution finished.")

    # Optional: Exit with status code based on success
    sys.exit(0 if workflow_status else 1)