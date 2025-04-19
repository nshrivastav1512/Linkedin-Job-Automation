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
    print("[Startup] Successfully imported phase modules.")
except ImportError as e:
    print(f"!!!!!! ERROR: Failed to import one or more phase scripts: {e} !!!!!")
    print("Ensure the following files exist in the same directory as main_workflow.py:")
    print(" - phase1_list_scraper.py")
    print(" - phase2_detail_scraper.py")
    print(" - phase3_ai_analysis.py")
    print(" - phase4_tailoring.py")
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
    "excel_filepath": BASE_DIR / "linkedin_jobs_master_list.xlsx", # Changed name for clarity
    # MANDATORY: Your base resume in TXT format (for AI analysis in Phase 3).
    "resume_filepath_txt": BASE_DIR / "Resume.txt",
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
# Add this comment block near the CONFIG_SELENIUM definition
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
    # OPTIONAL: Wait time in seconds for elements to appear (longer waits, e.g., page loads).
    "wait_time_long": 30,
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
    "search_term": "SQL Developer",
    # OPTIONAL: The location to search within (e.g., "City, State, Country" or "Country"). Leave empty ('') if not needed.
    "search_location_text": "Pune, Maharashtra, India",
    # OPTIONAL: LinkedIn's internal Geo ID for the location (more precise). Find using network tools or online resources. Leave empty ('') if using location_text only or no location.
    "search_geo_id": "",#114806696
    # MANDATORY: Date filter choice. '1': Any time, '2': Past month, '3': Past week, '4': Past 24 hours.
    "date_filter_choice": "4", # Defaulting to 'Past Week'
    # OPTIONAL: Set to True to scrape multiple pages (up to max_pages). False scrapes only the first page.
    "scrape_all_pages": True, # Changed default to True as it's more common
    # OPTIONAL: Maximum number of pages to scrape if scrape_all_pages is True. LinkedIn typically limits to 40.
    "max_pages_to_scrape": 10, # Reduced default for faster testing
    # OPTIONAL: Save the Excel file after each page is scraped (more robust but slower).
    "save_after_each_page": True,
    # OPTIONAL: Set to True for detailed console output during card extraction (can be noisy).
    "verbose_card_extraction": False,
    # OPTIONAL: Limit the number of *successfully* scraped jobs per page. Set to 0 or None for no limit per page.
    "jobs_per_page_limit": 0,
    # OPTIONAL: Limit the total number of *successfully* scraped jobs for the entire Phase 1 run. Set to 0 or None for no total limit.
    "total_jobs_limit": 100, # Example: Set to 100 to stop after 100 jobs are scraped
}

# --- 4. Phase 2: Job Detail Scraping Configuration ---
CONFIG_PHASE2 = {
    # OPTIONAL: Save progress to Excel every N jobs processed. Helps resume if interrupted.
    "save_interval": 5,
}

# --- 5. Phase 3 & 4: AI (Gemini) Configuration ---
CONFIG_AI = {
    # MANDATORY: The name of the API key variable in your .env file.
    "api_key_name": "GEMINI_API_KEY",
    # MANDATORY: Model for extracting structured data (responsibilities, skills) from JD. Flash is faster/cheaper.
    "extraction_model_name": "gemini-2.0-flash",
    # MANDATORY: Model for the detailed resume vs. JD analysis and scoring. Pro often yields better results here.
    "analysis_model_name": "gemini-2.0-flash", # Changed to Pro as analysis is complex
    # MANDATORY: Model for generating the tailored resume content (summary, bullets, skills). Pro is recommended for quality.
    "tailoring_model_name": "gemini-2.0-flash",
    # OPTIONAL: Delay (in seconds) between consecutive Gemini API calls to help avoid rate limits.
    "api_delay_seconds": 8, # Increased slightly for Pro models
    # OPTIONAL: Safety settings for Gemini API calls. BLOCK_NONE can be risky but sometimes needed for job/resume text. Review Google's policy.
    "safety_settings": {
        "HARM_CATEGORY_HARASSMENT": "BLOCK_NONE",
        "HARM_CATEGORY_HATE_SPEECH": "BLOCK_NONE",
        "HARM_CATEGORY_SEXUALLY_EXPLICIT": "BLOCK_NONE",
        "HARM_CATEGORY_DANGEROUS_CONTENT": "BLOCK_NONE",
    },
    # OPTIONAL: Generation config for API calls expecting JSON output (tailoring, extraction).
    "generation_config_json": {
        "temperature": 0.7, # Slightly lower temp for more deterministic JSON structure
        "top_p": 1,
        "top_k": 1,
        "max_output_tokens": 8192, # Increased max tokens
        "response_mime_type": "application/json",
    },
    # OPTIONAL: Generation config for API calls expecting Text output (analysis).
    "generation_config_text": {
        "temperature": 0.7, # Moderate temp for creative but focused text
        "top_p": 1,
        "top_k": 1,
        "max_output_tokens": 8192, # Increased max tokens
        # "response_mime_type": "text/plain", # Default, usually not needed
    },
    # MANDATORY: Path to your base resume text file (used as input for analysis).
    # This is duplicated here for clarity, but uses the path from CONFIG_PATHS.
    "resume_html_filepath": CONFIG_PATHS["resume_filepath_html"],
}

# --- 6. Phase 4: Resume Tailoring Configuration ---
CONFIG_PHASE4 = {
    # MANDATORY: Minimum AI Match Score (from Phase 3) required to trigger resume tailoring. Adjust based on desired selectivity.
    "score_threshold": 2.5, # Example: Moderate fit (3 stars) or higher
    # OPTIONAL: Maximum number of attempts the AI will make to tailor and fit the resume onto one page.
    "max_tailoring_attempts": 3,
    # OPTIONAL: Save progress to Excel every N resumes tailored.
    "save_interval": 3,
    # MANDATORY: Path to your base HTML resume template file (used as input for tailoring).
    # This is duplicated here for clarity, but uses the path from CONFIG_PATHS.
    "html_template_filepath": CONFIG_PATHS["resume_filepath_html"],
}

# --- 7. Status Flags ---
# Standardized status values used in the 'Status' column of the Excel file.
CONFIG_STATUS = {
    "NEW": "New", # Job added, needs details scraped
    "PROCESSING_DETAILS": "Processing Details", # Phase 2 currently working on this
    "READY_FOR_AI": "Ready for AI", # Details scraped, ready for Phase 3
    "PROCESSING_AI": "Processing AI Analysis", # Phase 3 currently working on this
    "AI_ANALYZED": "AI Analyzed", # AI analysis complete, ready for Phase 4 (if score is high enough)
    "TAILORING": "Tailoring Resume", # Phase 4 currently working on this
    "SUCCESS": "Tailored Resume Created", # Phase 4 finished successfully (PDF generated)
    "NEEDS_EDIT": "Tailoring Needs Manual Edit", # Phase 4 finished, but PDF likely > 1 page
    "SKIPPED_LOW_SCORE": "Skipped - Low AI Score", # Phase 4 skipped due to score below threshold Error Statuses
    "FAILED_SCRAPE_LIST": "Error - Scrape Job List", # Generic error during Phase 1
    "FAILED_SCRAPE_DETAILS": "Error - Scrape Job Details", # Error during Phase 2 detail scraping
    "FAILED_AI_EXTRACTION": "Error - AI Extraction", # Error during Phase 3 data extraction call
    "FAILED_AI_ANALYSIS": "Error - AI Analysis", # Error during Phase 3 analysis/scoring call
    "FAILED_TAILORING": "Error - AI Tailoring", # Error during Phase 4 tailoring API call
    "FAILED_HTML_EDIT": "Error - HTML Edit", # Error applying AI edits to HTML in Phase 4
    "FAILED_PDF_GEN": "Error - PDF Generation", # Error generating PDF in Phase 4
    "FAILED_FILE_ACCESS": "Error - File Access", # Permission error, file not found etc.
    "FAILED_API_CONFIG": "Error - API Config/Auth", # Cannot connect to API
    "FAILED_WEBDRIVER": "Error - WebDriver Connection", # Cannot connect to Chrome
    "INVALID_LINK": "Error - Invalid Job Link", # Bad link found in Excel
    "MISSING_DATA": "Error - Missing Input Data", # e.g., JD text missing before AI phase
    "UNKNOWN_ERROR": "Error - Unknown", # Catch-all for unexpected issues
}

# --- 8. LinkedIn Selectors ---
# Centralized CSS selectors for LinkedIn elements. Update these if LinkedIn changes its HTML structure.
# Using more robust selectors where possible.
# Last Updated: 2025-04-17 (Based on user-provided HTML)
CONFIG_LINKEDIN_SELECTORS = {
    # --- Phase 1: Job List Page ---
    "job_list_container": "div.scaffold-layout__list",
    "job_card": "li.scaffold-layout__list-item[data-occludable-job-id]",
    "job_card_link": "a.job-card-list__title, a.job-card-container__link, a.base-card__full-link",
    "job_card_title": "strong",
    "job_card_company": "div.artdeco-entity-lockup__subtitle span",
    "job_card_location": "ul.job-card-container__metadata-wrapper li:first-child span",
    "job_card_logo": "img.artdeco-entity-image",
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
    # --- UPDATED: Targeting the div with padding containing the core top card elements ---
    "details_top_card": "div.p5",
    # ------------------------------------------------------------------------------------
    "details_company_link": ".job-details-jobs-unified-top-card__company-name a", # Seems correct
    "details_metadata_container": ".job-details-jobs-unified-top-card__primary-description-container",# Seems correct
    "details_posted_ago_fallback": "span.jobs-unified-top-card__posted-date", # Less likely used now
    "details_easy_apply_button": "button.jobs-apply-button[aria-label*='Easy Apply']", # Seems correct
    "details_description_container": "div#job-details", # Seems correct
    "details_show_more_button": "button.jobs-description__footer-button", # Updated selector
    "details_company_section": "section.jobs-company", # Seems correct
    "details_company_followers_subtitle": "div.artdeco-entity-lockup__subtitle", # Seems correct
    "details_company_info_div": "div.t-14.mt5", # Seems correct
    "details_company_about_text": "p.jobs-company__company-description", # Seems correct
    # --- UPDATED: Targeting button class for company desc show more ---
    "details_company_show_more_button": "button.inline-show-more-text__button",
    # -----------------------------------------------------------------
    "details_hiring_team_section_xpath": "//h2[normalize-space()='Meet the hiring team']/following-sibling::div", # Plausible
    "details_hiring_team_card": "div.display-flex.align-items-center.mt4", # Plausible
    # --- UPDATED: Targeting strong tag within the name span ---
    "details_hiring_team_name": "span.jobs-poster__name strong",
    # -------------------------------------------------------
    "details_hiring_team_profile_link": "a[href*='/in/']", # Plausible
}
# ---9. Workflow Control ---
CONFIG_WORKFLOW = {
    # MANDATORY: First phase to execute (1, 2, 3, or 4). Set > 1 to skip earlier phases.
    "start_phase": 1,
    # MANDATORY: Last phase to execute (1, 2, 3, or 4).
    "end_phase": 4,
    # OPTIONAL: Set True to retry processing rows that previously failed in Phase 2.
    "retry_failed_phase2": True,
    # OPTIONAL: Set True to retry processing rows that previously failed in Phase 3.
    "retry_failed_phase3": True,
    # OPTIONAL: Set True to retry processing rows that previously failed in Phase 4 (Tailoring/PDF).
    "retry_failed_phase4": True,
}


# ==============================================================================
# --- Master Configuration Dictionary ---
# Combine all configuration pieces into a single dictionary for easy passing.
# ==============================================================================
MASTER_CONFIG = {
    "paths": CONFIG_PATHS,
    "selenium": CONFIG_SELENIUM,
    # --- ADD THIS ---
    "workflow": CONFIG_WORKFLOW,
    # -------------
    "phase1": CONFIG_PHASE1,
    "phase2": CONFIG_PHASE2,
    "ai": CONFIG_AI,
    "phase4": CONFIG_PHASE4,
    "status": CONFIG_STATUS,
    "selectors": CONFIG_LINKEDIN_SELECTORS,
}

# ==============================================================================
# --- Logging Setup ---
# ==============================================================================
def setup_logging(config):
    """Configures logging to console and a dated file."""
    log_folder = config['paths']['log_folder']
    search_term_safe = "".join(c if c.isalnum() else "_" for c in config['phase1']['search_term'])
    location_safe = "".join(c if c.isalnum() else "_" for c in config['phase1']['search_location_text'])[:20] # Limit length
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"log_{timestamp}_{search_term_safe}_{location_safe}.log"
    log_filepath = log_folder / log_filename

    # Create log directory if it doesn't exist
    try:
        log_folder.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"!!!!!! ERROR: Could not create log directory: {log_folder}. Error: {e} !!!!!")
        print("Logging to console only.")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)-8s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        return

    # Configure logging
    logging.basicConfig(
        level=logging.INFO, # Set base level to INFO
        format='%(asctime)s - %(levelname)-8s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(log_filepath, mode='a', encoding='utf-8'), # Append to log file
            logging.StreamHandler() # Also print to console
        ]
    )
    # Silence noisy libraries if needed (optional)
    # logging.getLogger("urllib3").setLevel(logging.WARNING)
    # logging.getLogger("selenium").setLevel(logging.INFO)

     # --- Add these lines to silence noisy libraries ---
    # Silence WeasyPrint INFO and WARNING messages (optional: set to ERROR to silence more)
    logging.getLogger("weasyprint").setLevel(logging.ERROR)
    # Silence fontTools INFO/DEBUG messages (often used by WeasyPrint/woff2)
    logging.getLogger("fontTools").setLevel(logging.WARNING)
    # Silence woff2 INFO messages (like BytesIO processing)
    logging.getLogger("woff2").setLevel(logging.WARNING)
    # -------------------------------------------------

    logging.info("=================================================")
    logging.info(f"Logging initialized. Log file: {log_filepath}")
    logging.info("Starting Job Automation Workflow")
    logging.info(f"Base Directory: {config['paths']['base_dir']}")
    logging.info(f"Using Excel File: {config['paths']['excel_filepath']}")
    logging.info(f"Search Term: '{config['phase1']['search_term']}'")
    logging.info(f"Location: '{config['phase1']['search_location_text']}' (GeoID: {config['phase1']['search_geo_id']})")
    logging.info("=================================================")

# ==============================================================================
# --- Main Workflow Orchestration ---
# ==============================================================================
# **** START REPLACEMENT for run_workflow function in main_workflow.py ****
def run_workflow(config):
    """Runs the phases sequentially, respecting start/end phase config."""
    logging.info("########## Starting Workflow ##########")
    start_phase = config['workflow']['start_phase']
    end_phase = config['workflow']['end_phase']
    logging.info(f"Workflow configured to run from Phase {start_phase} to Phase {end_phase}.")

    overall_success = True # Assume success unless a critical error occurs
    phase_times = {}
    phases_to_run = range(start_phase, end_phase + 1)

    # --- Phase 1: Scrape Job List ---
    if 1 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 1: Scrape Job List ---")
        try:
            success_phase1 = phase1_list_scraper.run_phase1_job_list_scraping(config)
            if not success_phase1:
                logging.error("Phase 1 failed critically. Aborting workflow.")
                overall_success = False
            else:
                logging.info("--- Phase 1 Completed ---")
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 1 execution: {e}", exc_info=True)
            overall_success = False
        phase_times['Phase 1'] = time.time() - phase_start_time
        logging.info(f"Phase 1 duration: {phase_times.get('Phase 1', 0):.2f} seconds.")
    else:
        logging.info("--- Skipping Phase 1 ---")

    # --- Phase 2: Scrape Job Details ---
    if overall_success and 2 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 2: Scrape Job Details ---")
        try:
            success_phase2 = phase2_detail_scraper.run_phase2_detail_scraping(config)
            if not success_phase2:
                logging.warning("Phase 2 encountered critical errors (check logs). Proceeding cautiously if possible.")
                # Decide if Phase 2 failure halts the whole workflow
                # overall_success = False # Uncomment to make Phase 2 failure critical
            else:
                logging.info("--- Phase 2 Completed ---")
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 2 execution: {e}", exc_info=True)
            overall_success = False
        phase_times['Phase 2'] = time.time() - phase_start_time
        logging.info(f"Phase 2 duration: {phase_times.get('Phase 2', 0):.2f} seconds.")
    elif 2 in phases_to_run: # Log skip only if it was supposed to run but previous failed
         logging.warning("--- Skipping Phase 2 due to previous critical failure ---")
    else:
         logging.info("--- Skipping Phase 2 ---")

    # --- Phase 3: AI Analysis & Scoring ---
    if overall_success and 3 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 3: AI Analysis & Scoring ---")
        try:
            success_phase3 = phase3_ai_analysis.run_phase3_ai_processing(config)
            if not success_phase3:
                logging.warning("Phase 3 encountered critical errors (check logs). Proceeding cautiously if possible.")
                # overall_success = False # Uncomment if Phase 3 must fully succeed
            else:
                 logging.info("--- Phase 3 Completed ---")
        except ImportError as e: # Catch library import errors specifically
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
    else:
         logging.info("--- Skipping Phase 3 ---")


    # --- Phase 4: AI Tailoring & PDF Generation ---
    if overall_success and 4 in phases_to_run:
        phase_start_time = time.time()
        logging.info("--- Phase 4: AI Resume Tailoring & PDF Generation ---")
        try:
            success_phase4 = phase4_tailoring.run_phase4_resume_tailoring(config)
            if not success_phase4:
                logging.warning("Phase 4 encountered critical errors (check logs and 'Tailored_Resumes' folder).")
                # Phase 4 handles many row-level errors, so don't necessarily set overall_success=False
            else:
                 logging.info("--- Phase 4 Completed ---")
        except ImportError as e: # Catch library import errors specifically
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
    else:
         logging.info("--- Skipping Phase 4 ---")


    # --- Workflow End ---
    logging.info("#################################################")
    if overall_success:
        logging.info(f"Job Automation Workflow Completed (Phases {start_phase}-{end_phase}).")
    else:
        logging.error("Job Automation Workflow Halted or Completed with CRITICAL ERRORS.")
    logging.info("Review log file for detailed information.")
    logging.info("#################################################")
    return overall_success, phase_times
# **** END REPLACEMENT for run_workflow function in main_workflow.py ****
    """Runs the phases sequentially, handling errors and logging."""
    logging.info("########## Starting Workflow ##########")
    overall_success = True
    phase_times = {}

    # --- Phase 1: Scrape Job List ---
    phase_start_time = time.time()
    logging.info("--- Phase 1: Scrape Job List ---")
    try:
        # Phase 1 connects to browser, searches, scrapes list, adds to Excel
        success_phase1 = phase1_list_scraper.run_phase1_job_list_scraping(config)
        if not success_phase1:
            logging.error("Phase 1 failed critically. Aborting workflow.")
            overall_success = False
        else:
            logging.info("--- Phase 1 Completed ---")
    except Exception as e:
        logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 1 execution: {e}")
        logging.critical(traceback.format_exc())
        overall_success = False
    phase_times['Phase 1'] = time.time() - phase_start_time
    logging.info(f"Phase 1 duration: {phase_times['Phase 1']:.2f} seconds.")

    # --- Phase 2: Scrape Job Details ---
    if overall_success:
        phase_start_time = time.time()
        logging.info("--- Phase 2: Scrape Job Details ---")
        try:
            # Phase 2 connects, reads Excel, finds 'New', scrapes details, updates status
            success_phase2 = phase2_detail_scraper.run_phase2_detail_scraping(config)
            if not success_phase2:
                # Phase 2 is designed to handle row-level errors, so even False might mean partial success.
                logging.warning("Phase 2 encountered errors processing some jobs (check logs). Proceeding.")
                # Decide if *any* error in Phase 2 should halt the process:
                # overall_success = False # Uncomment to make Phase 2 failure critical
            else:
                logging.info("--- Phase 2 Completed ---")
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 2 execution: {e}")
            logging.critical(traceback.format_exc())
            overall_success = False # Critical if the whole phase function crashes
        phase_times['Phase 2'] = time.time() - phase_start_time
        logging.info(f"Phase 2 duration: {phase_times['Phase 2']:.2f} seconds.")

    # --- Phase 3: AI Analysis & Scoring ---
    if overall_success:
        phase_start_time = time.time()
        logging.info("--- Phase 3: AI Analysis & Scoring ---")
        try:
            # Phase 3 reads Excel, finds 'Ready for AI', calls Gemini, updates status
            success_phase3 = phase3_ai_analysis.run_phase3_ai_processing(config)
            if not success_phase3:
                logging.warning("Phase 3 encountered errors processing some jobs (check logs). Proceeding.")
                # overall_success = False # Uncomment if Phase 3 must fully succeed
            else:
                 logging.info("--- Phase 3 Completed ---")
        except ImportError as e:
             if 'google.generativeai' in str(e):
                 logging.critical("CRITICAL ERROR: google-generativeai library not installed. Run 'pip install google-generativeai'.")
             else:
                  logging.critical(f"CRITICAL UNHANDLED ImportError during Phase 3: {e}")
                  logging.critical(traceback.format_exc())
             overall_success = False
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 3 execution: {e}")
            logging.critical(traceback.format_exc())
            overall_success = False
        phase_times['Phase 3'] = time.time() - phase_start_time
        logging.info(f"Phase 3 duration: {phase_times['Phase 3']:.2f} seconds.")

    # --- Phase 4: AI Tailoring & PDF Generation ---
    if overall_success:
        phase_start_time = time.time()
        logging.info("--- Phase 4: AI Resume Tailoring & PDF Generation ---")
        try:
            # Phase 4 reads Excel, filters, tailors, generates files, updates status
            success_phase4 = phase4_tailoring.run_phase4_resume_tailoring(config)
            if not success_phase4:
                logging.warning("Phase 4 encountered errors processing some jobs (check logs and 'Tailored_Resumes' folder).")
                # This phase often has partial successes/failures, so don't set overall_success=False unless needed
            else:
                 logging.info("--- Phase 4 Completed ---")
        except ImportError as e:
             if 'weasyprint' in str(e):
                 logging.critical("CRITICAL ERROR: WeasyPrint library not installed or missing system dependencies (GTK+). See WeasyPrint documentation for installation.")
             elif 'PyPDF2' in str(e):
                 logging.critical("CRITICAL ERROR: PyPDF2 library not installed. Run 'pip install pypdf2'.")
             else:
                  logging.critical(f"CRITICAL UNHANDLED ImportError during Phase 4: {e}")
                  logging.critical(traceback.format_exc())
             overall_success = False
        except Exception as e:
            logging.critical(f"CRITICAL UNHANDLED ERROR during Phase 4 execution: {e}")
            logging.critical(traceback.format_exc())
            overall_success = False
        phase_times['Phase 4'] = time.time() - phase_start_time
        logging.info(f"Phase 4 duration: {phase_times['Phase 4']:.2f} seconds.")

    # --- Workflow End ---
    logging.info("#################################################")
    if overall_success:
        logging.info("Job Automation Workflow Completed SUCCESSFULLY.")
    else:
        logging.error("Job Automation Workflow Completed with CRITICAL ERRORS in one or more phases.")
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

    # Setup logging *after* loading config potentially containing log paths/names
    setup_logging(MASTER_CONFIG)

    # Run the main workflow
    workflow_status, phase_durations = run_workflow(MASTER_CONFIG)

    global_end_time = time.time()
    total_runtime = global_end_time - global_start_time
    logging.info(f"Total Workflow Runtime: {total_runtime:.2f} seconds.")
    logging.info(f"Phase Durations: {phase_durations}")
    logging.info("Script execution finished.")

    # Optional: Exit with status code based on success
    sys.exit(0 if workflow_status else 1)