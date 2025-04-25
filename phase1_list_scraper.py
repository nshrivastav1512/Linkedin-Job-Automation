# phase1_list_scraper.py
# Phase 1: Scrapes basic job listing information from LinkedIn search results.

import time
import os
import traceback
import re
import logging
import random
from urllib.parse import quote_plus
from datetime import datetime
import pandas as pd
import zipfile # To handle BadZipFile exception
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
    ElementNotInteractableException
)

# MODIFIED (Proposals #7, #8, #10, #11): Define ALL columns expected in the final Excel sheet across all phases
# Ensures the initial file has the complete structure.
ALL_EXPECTED_COLUMNS = [
    # Core Job Info
    'Job ID', 'Title', 'Company', 'Location', 'Workplace Type', 'Link', 'Easy Apply', 'Promoted', 'Viewed',
    'Early Applicant', 'Verified', 'Posted Ago Text', 'Posted Days Ago', 'Posted Hours Ago', 'Salary Range',
    'Insights', #'Company Logo URL', # REMOVED
    'Source', 'Date Added', 'Status', 'Applied Date', 'Notes',
    # Phase 2 Detailed Info
    'Applicant Count', 'Job Description HTML', 'Job Description Plain Text', 'About Company',
    'Date Scraped Detailed', 'Posted Ago Text Detailed', 'Company LinkedIn URL', 'Company Industry',
    'Company Size', 'Company LinkedIn Members', 'Company Followers',
    'Hiring Team Member 1 Name', # Member 2 info appended here if found
    'Hiring Team Member 1 Profile URL', # Member 2 info appended here if found
    #'Hiring Team Member 2 Name', # REMOVED
    #'Hiring Team Member 2 Profile URL', # REMOVED
    'Scraping Issues', # NEW (Proposal #7) - For Phase 2 issues
    #'Skills Required', # REMOVED (Using AI extracted skills)
    # Phase 3 - AI Extracted Text Outputs
    'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills',
    'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description',
    # Phase 3 - AI Analysis Outputs
    'AI Match Score', 'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement',
    'AI Actionable Recommendations',
    # Phase 3 - Detailed Score Columns
    'Keyword Match Score', 'Achievements Score', 'Summary Quality Score',
    'Structure Score', 'Tools Certs Score', 'Total Match Score',
    # Phase 4 Tailoring Output
    'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List',
    'Tailored HTML Path', 'Tailored PDF Path',
    'Tailored PDF Pages', # NEW (Proposal #8)
    # Phase 5 Rescoring Output
    'Tailored Resume Score', # NEW (Proposal #10)
    'Score Change', # NEW (Proposal #10)
    'Tailoring Effectiveness Status', # NEW (Proposal #10)
    'Retailoring Attempts', # NEW (Proposal #10)
]


# --- Helper Functions ---

def get_random_delay(config, delay_type="medium"):
    """Generates a random delay based on config settings."""
    if not config['selenium'].get('enable_random_delays', True):
        return 0.1 # Minimal delay if disabled

    if delay_type == "short":
        base = config['selenium']['delay_short_base']
        variance = config['selenium']['delay_short_variance']
    elif delay_type == "long":
        base = config['selenium']['delay_long_base']
        variance = config['selenium']['delay_long_variance']
    else: # medium
        base = config['selenium']['delay_medium_base']
        variance = config['selenium']['delay_medium_variance']

    return base + random.uniform(0, variance)

# MODIFIED (Proposal #2): Enhanced Chrome Debugger Connection Handling
def setup_selenium_driver(config):
    """Connects Selenium to an existing Chrome instance using config, with enhanced error handling."""
    driver_path = config['selenium']['chromedriver_path']
    port = config['selenium']['debugger_port']
    max_retries = 3
    retry_count = 0

    logging.info(f"Attempting to connect to existing Chrome on port {port}...")
    logging.info(f"Using ChromeDriver path: {driver_path}")

    if not os.path.exists(driver_path):
        logging.error(f"ChromeDriver executable not found at: {driver_path}")
        return None

    service = Service(executable_path=driver_path)
    options = webdriver.ChromeOptions()
    options.add_experimental_option("debuggerAddress", f"localhost:{port}")
    # Optional: Reduce log noise
    options.add_argument("--log-level=3")
    # options.add_experimental_option('excludeSwitches', ['enable-logging']) # Suppress DevTools listening message

    while retry_count < max_retries:
        try:
            driver = webdriver.Chrome(service=service, options=options)
            logging.info("Successfully connected to Chrome debugger.")
            # Basic check
            time.sleep(0.5)
            try:
                current_url = driver.current_url
                if not current_url:
                    logging.warning("Connected, but could not get current URL. Browser might be stuck or unresponsive.")
                elif "linkedin.com" not in current_url:
                    logging.warning(f"Connected, but the active tab is not LinkedIn ({current_url}). Ensure LinkedIn is open and active.")
                else:
                    logging.info(f"Confirmed connection on a LinkedIn page: {current_url}")
            except WebDriverException as url_err:
                 logging.warning(f"Connected, but encountered error getting current URL: {url_err}. Browser might be initializing.")
            return driver # Successful connection

        except WebDriverException as e:
            is_connection_error = any(err_msg in str(e).lower() for err_msg in [
                "failed to connect", "timed out", "cannot connect", "connection refused"
            ])

            if is_connection_error:
                retry_count += 1
                logging.error(f"WebDriverException connecting to Chrome on port {port} (Attempt {retry_count}/{max_retries}).")
                logging.error(f"Error details: {e}")

                if retry_count >= max_retries:
                    logging.error("Maximum connection retries reached.")
                    # Print detailed instructions only on final failure
                    print("\n" + "="*60)
                    print("!!!!!! FAILED TO CONNECT TO CHROME DEBUGGER !!!!!!")
                    print("Please ensure:")
                    print(" 1. ALL other Chrome instances are CLOSED.")
                    print(f" 2. Chrome was started MANUALLY using the command line with port {port}.")
                    print(" 3. The correct command was used for your OS:")
                    print("    Windows (Command Prompt - Adjust path if needed):")
                    print(f'       "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port={port} --user-data-dir="C:\\ChromeDebugProfile"')
                    print("    macOS (Terminal):")
                    print(f'       /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port={port} --user-data-dir="$HOME/ChromeDebugProfile"')
                    print("    Linux (Terminal):")
                    print(f'       google-chrome --remote-debugging-port={port} --user-data-dir="$HOME/ChromeDebugProfile"')
                    print(" 4. You logged into LinkedIn MANUALLY in that specific Chrome window.")
                    print("="*60 + "\n")
                    # No interactive retry prompt in this version, exits after max retries.
                    return None
                else:
                    logging.info(f"Retrying connection in 10 seconds...")
                    time.sleep(10)
                    continue # Go to next iteration of the while loop

            else:
                # Handle other WebDriverExceptions (like capability errors)
                logging.error(f"WebDriverException (Non-connection related) during setup: {e}")
                # logging.error(traceback.format_exc()) # Uncomment for more detail if needed
                return None # Exit on non-connection WebDriver errors

        except Exception as e:
            logging.error(f"An unexpected error occurred during Selenium setup: {e}")
            logging.error(traceback.format_exc())
            return None

    return None # Should only be reached if max_retries exceeded

def parse_posted_ago(text):
    """Parses 'Posted Ago' text into days and hours."""
    # (No changes needed in this function)
    if not isinstance(text, str): return -1, -1
    text_lower = text.lower()
    days, hours = -1, -1
    if "just now" in text_lower: days, hours = 0, 0
    elif "yesterday" in text_lower: days, hours = 1, 0
    else:
        match = re.search(r'(\d+)\s+(hour|day|week|month|year)s?', text_lower)
        if match:
            num, unit = int(match.group(1)), match.group(2)
            if unit == 'hour': hours, days = num % 24, num // 24
            elif unit == 'day': days, hours = num, 0
            elif unit == 'week': days, hours = num * 7, 0
            elif unit == 'month': days = num * 30 # Approx
            elif unit == 'year': days = num * 365 # Approx
    return days, hours

# MODIFIED (Proposal #11): Removed Company Logo URL and Skills Required extraction
'''def extract_job_data_from_card(card_element, config):

    """Extracts detailed data points from a single job card element using selectors from config."""
    selectors = config['selectors']
    verbose = config['phase1']['verbose_card_extraction']
    if verbose: logging.debug("Processing card...")

    data = { # Initialize with defaults, removed fields
        'Job ID': 'N/A', 'Title': 'N/A', 'Company': 'N/A', 'Location': 'N/A',
        'Workplace Type': 'N/A', 'Link': 'N/A', 'Easy Apply': False, 'Promoted': False,
        'Viewed': False, 'Early Applicant': False, 'Verified': False,
        'Posted Ago Text': 'N/A', 'Posted Days Ago': -1, 'Posted Hours Ago': -1,
        'Salary Range': 'N/A', 'Insights': 'N/A', #'Company Logo URL': 'N/A', # REMOVED
        'Source': 'LinkedIn Job Search'
    }

    # --- Essential Info: Link, Title, Job ID ---
    # (Logic remains the same)
    try:
        link_element = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_link'])
        data['Link'] = link_element.get_attribute('href')
        if data['Link'] and '?' in data['Link']: data['Link'] = data['Link'].split('?')[0]
        try: data['Title'] = link_element.find_element(By.CSS_SELECTOR, selectors['job_card_title']).text.strip()
        except NoSuchElementException: data['Title'] = link_element.text.strip()
        try:
            parent_li = link_element.find_element(By.XPATH, './ancestor::li')
            data['Job ID'] = parent_li.get_attribute('data-occludable-job-id') or parent_li.get_attribute('data-entity-urn')
            if data['Job ID'] and 'jobPosting:' in data['Job ID']: data['Job ID'] = data['Job ID'].split(':')[-1]
        except Exception: data['Job ID'] = 'N/A'
        if not data['Link'] or data['Link'] == 'N/A' or not data['Title'] or data['Title'] == 'N/A':
             logging.warning(f"Card skipped: Missing essential Link ('{data['Link']}') or Title ('{data['Title']}').")
             return None
        if verbose: logging.debug(f"  Found Link: {data['Link']}, Title: {data['Title']}, JobID: {data['Job ID']}")
    except NoSuchElementException: logging.warning(f"Card skipped: Essential selector '{selectors['job_card_link']}' failed."); return None
    except Exception as e: logging.error(f"Card skipped: Unexpected error getting essential info: {e}", exc_info=True); return None

    # --- Other Fields (Robust extraction) ---
    # (Logic remains the same for Company, Location, Verified, Footer, Salary, Insights)
    try: data['Company'] = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_company']).text.strip()
    except Exception: data['Company'] = 'N/A (Company)'
    try:
        location_element = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_location'])
        job_location_raw = location_element.text.strip()
        match = re.search(r'\((On-site|Remote|Hybrid)\)', job_location_raw, re.IGNORECASE)
        if match:
            data['Workplace Type'] = match.group(1).capitalize()
            data['Location'] = job_location_raw.replace(match.group(0), '').strip(' ·,')
        else: data['Location'] = job_location_raw
        if data['Company'] != 'N/A (Company)' and data['Company'] in data['Location']: data['Location'] = data['Location'].replace(data['Company'], '').strip(' ·,')
    except Exception: data['Location'] = 'N/A (Location)'
    try: card_element.find_element(By.CSS_SELECTOR, selectors['job_card_verified_icon']); data['Verified'] = True
    except NoSuchElementException: data['Verified'] = False
    except Exception: data['Verified'] = False

    # --- Logo: REMOVED ---
    # try:
    #     logo_img = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_logo'])
    #     data['Company Logo URL'] = logo_img.get_attribute('src')
    # except Exception: data['Company Logo URL'] = 'N/A'

    try: # Footer Info
        footer_element = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_footer_list'])
        footer_items = footer_element.find_elements(By.TAG_NAME, 'li')
        for item in footer_items:
            item_text_lower = item.text.lower()
            if "easy apply" in item_text_lower: data['Easy Apply'] = True
            if "viewed" in item_text_lower: data['Viewed'] = True
            if "promoted" in item_text_lower: data['Promoted'] = True
            if "early applicant" in item_text_lower: data['Early Applicant'] = True
            try:
                time_tag = item.find_element(By.TAG_NAME, 'time')
                data['Posted Ago Text'] = time_tag.text.strip()
            except NoSuchElementException:
                 if any(word in item_text_lower for word in ["ago", "hour", "day", "week", "month", "yesterday", "just now"]):
                     data['Posted Ago Text'] = item.text.strip()
        if data['Posted Ago Text'] != 'N/A': data['Posted Days Ago'], data['Posted Hours Ago'] = parse_posted_ago(data['Posted Ago Text'])
    except NoSuchElementException: pass
    except Exception as footer_err: logging.warning(f"  Error parsing footer info: {footer_err}")

    try: data['Salary Range'] = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_salary']).text.strip()
    except Exception: data['Salary Range'] = 'N/A'
    try: data['Insights'] = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_insights']).text.strip()
    except Exception: data['Insights'] = 'N/A'

    # --- Skills Required: REMOVED ---

    if verbose: logging.debug(f"  Extracted Data: {data}")
    return data
'''

# File: phase1_list_scraper.py

# --- Helper Function (Recommended) ---
def safe_find_element(parent_element, by, selector):
    try:
        return parent_element.find_element(by, selector)
    except NoSuchElementException:
        return None

# --- CORRECTED extract_job_data_from_card ---
def extract_job_data_from_card(card_element, config):
    """Extracts detailed data points from a single job card element using selectors from config."""
    selectors = config['selectors']
    verbose = config['phase1']['verbose_card_extraction']
    if verbose: logging.debug("Processing card...")

    data = { # Initialize with correct defaults
        'Job ID': 'N/A', 'Title': 'N/A', 'Company': 'N/A', 'Location': 'N/A',
        'Workplace Type': 'N/A', 'Link': 'N/A', 'Easy Apply': False, 'Promoted': False, # Default FALSE
        'Viewed': False, 'Early Applicant': False, 'Verified': False, # Default FALSE
        'Posted Ago Text': '', 'Posted Days Ago': -1, 'Posted Hours Ago': -1,
        'Salary Range': '', 'Insights': '',
        'Source': 'LinkedIn Job Search'
    }

    # --- Essential Info: Link, Title, Job ID ---
    try:
        # Use the original, more general link selectors
        link_element = safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_link'].split(',')[0]) or \
                       safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_link'].split(',')[1]) or \
                       safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_link'].split(',')[2])

        if not link_element:
            logging.warning("Card skipped: Could not find primary link element using any selector.")
            return None

        data['Link'] = link_element.get_attribute('href')
        if data['Link'] and '?' in data['Link']: data['Link'] = data['Link'].split('?')[0]

        # Extract Title
        title_el = safe_find_element(link_element, By.CSS_SELECTOR, selectors['job_card_title'])
        data['Title'] = title_el.text.strip() if title_el else link_element.text.strip() # Fallback to link text

        # Extract Job ID
        job_id_urn = card_element.get_attribute('data-entity-urn')
        job_id_occludable = card_element.get_attribute('data-occludable-job-id')
        if job_id_urn and 'jobPosting:' in job_id_urn: data['Job ID'] = job_id_urn.split(':')[-1]
        elif job_id_occludable: data['Job ID'] = job_id_occludable
        else: id_match = re.search(r'/jobs/view/(\d+)/', data['Link'] or ""); data['Job ID'] = id_match.group(1) if id_match else 'N/A'

        if not data['Link'] or not data['Title'] or data['Job ID'] == 'N/A':
             logging.warning(f"Card skipped: Missing essential Link/Title/JobID.")
             return None
        if verbose: logging.debug(f"  Found Link: {data['Link']}, Title: {data['Title']}, JobID: {data['Job ID']}")
    except Exception as e: logging.error(f"Card skipped: Unexpected error getting essential info: {e}", exc_info=True); return None

    # --- Other Fields ---
    company_el = safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_company'])
    data['Company'] = company_el.text.strip() if company_el else 'N/A'

    # Location: Use the original selector logic but handle potential errors getting span
    location_li_el = safe_find_element(card_element, By.CSS_SELECTOR, "ul.job-card-container__metadata-wrapper li:first-child")
    if location_li_el:
        location_span_el = safe_find_element(location_li_el, By.TAG_NAME, "span") # Find span within the li
        if location_span_el:
            job_location_raw = location_span_el.text.strip()
            match = re.search(r'\((On-site|Remote|Hybrid)\)', job_location_raw, re.IGNORECASE)
            if match: data['Workplace Type'] = match.group(1).capitalize(); data['Location'] = job_location_raw.replace(match.group(0), '').strip(' ·,')
            else: data['Location'] = job_location_raw
            if data['Company'] != 'N/A' and data['Company'] in data['Location']: data['Location'] = data['Location'].replace(data['Company'], '').strip(' ·,')
        else: data['Location'] = location_li_el.text.strip() # Fallback to LI text if span fails
    else: data['Location'] = 'N/A'

    # --- Verified Badge ---
    # Search within the card_element context
    verified_el = safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_verified_icon'])
    data['Verified'] = bool(verified_el) # True only if element is found

    # --- Footer Info ---
    footer_list_el = safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_footer_list'])
    posted_ago_text = '' # Initialize as empty string
    if footer_list_el:
        try:
            # Try finding the time tag directly within the footer list first
            time_tag = safe_find_element(footer_list_el, By.TAG_NAME, 'time')
            if time_tag:
                posted_ago_text = time_tag.text.strip()
                if verbose: logging.debug(f"  Found Posted Ago via <time>: '{posted_ago_text}'")

            # Check list items for boolean flags and date fallback
            footer_items = footer_list_el.find_elements(By.TAG_NAME, 'li') # Get all items
            for item in footer_items:
                item_text = item.text # Get text once per item
                item_text_lower = item_text.lower()
                # Use precise checks for flags - check if the *whole* text matches common patterns
                if "easy apply" == item_text_lower.strip(): data['Easy Apply'] = True
                if "promoted" == item_text_lower.strip(): data['Promoted'] = True
                # Check for "Viewed" state (often bold) - check if the element itself is bold maybe?
                if "viewed" == item_text_lower.strip():
                     data['Viewed'] = True
                     # Check if the element has a bold style (more reliable than just text)
                     try:
                         if item.value_of_css_property('font-weight') in ['700', 'bold']:
                              data['Viewed'] = True
                              if verbose: logging.debug("  Found 'Viewed' state (bold).")
                         else: data['Viewed'] = False # Reset if not bold
                     except: pass # Ignore style check errors
                if "early applicant" == item_text_lower.strip(): data['Early Applicant'] = True

                # Fallback for date text ONLY if <time> tag wasn't found above
                if not posted_ago_text and any(word in item_text_lower for word in ["ago", "hour", "day", "week", "month", "yesterday", "just now"]):
                    posted_ago_text = item_text.strip()
                    if verbose: logging.debug(f"  Found Posted Ago via LI text fallback: '{posted_ago_text}'")
                    # Don't break here, continue checking other LIs for boolean flags

        except Exception as footer_err:
            logging.warning(f"  Error parsing footer info: {footer_err}")

    # Process posted_ago_text if found
    if posted_ago_text:
        data['Posted Ago Text'] = posted_ago_text
        data['Posted Days Ago'], data['Posted Hours Ago'] = parse_posted_ago(posted_ago_text)
    elif verbose:
         logging.debug("  Posted Ago text not found in footer.")

    # --- Salary ---
    salary_el = safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_salary'])
    data['Salary Range'] = salary_el.text.strip() if salary_el else ''

    # --- Insights ---
    insights_el = safe_find_element(card_element, By.CSS_SELECTOR, selectors['job_card_insights'])
    data['Insights'] = insights_el.text.strip() if insights_el else ''

    if verbose: logging.debug(f"  Final Extracted Data: {data}")
    return data

# MODIFIED (Proposals #3, #5, #6): Update logging, handle min unique target, track totals
def search_and_scrape_jobs(driver: webdriver, config: dict) -> tuple[list, int, int]:
    """Searches jobs, handles pagination, scrapes results, respects limits, tracks added/skipped counts."""
    # --- Config Extraction ---
    phase1_cfg = config['phase1']
    selenium_cfg = config['selenium']
    selectors = config['selectors']

    search_query = phase1_cfg['search_term']
    location_text = phase1_cfg['search_location_text']
    geo_id = phase1_cfg['search_geo_id']
    date_filter_choice = phase1_cfg['date_filter_choice']
    scrape_all = phase1_cfg['scrape_all_pages']
    max_pages = phase1_cfg['max_pages_to_scrape']
    save_each_page = phase1_cfg['save_after_each_page']
    wait_time_container = selenium_cfg['wait_time_long']
    wait_time_cards = selenium_cfg['wait_time_short']
    wait_time_element = selenium_cfg.get('wait_time_element', 3)
    jobs_per_page_limit = phase1_cfg.get('jobs_per_page_limit', 0) or None
    total_jobs_limit = phase1_cfg.get('total_jobs_limit', 0) or None
    minimum_unique_jobs_target = phase1_cfg.get('minimum_unique_jobs_target', 0) or 0 # Proposal #6
    jobs_per_linkedin_page = 25

    date_filter_map = {'1': None, '2': 'r2592000', '3': 'r604800', '4': 'r86400'}
    date_filter_url_param = date_filter_map.get(date_filter_choice)

    logging.info("Starting job search...")
    logging.info(f"Query='{search_query}', Location='{location_text}', GeoID='{geo_id}', DateFilter='{date_filter_url_param}'")
    logging.info(f"ScrapeAll={scrape_all}, MaxPages={max_pages}, TotalJobsLimit={total_jobs_limit}, MinUniqueTarget={minimum_unique_jobs_target}")

    all_scraped_jobs_data = [] # Stores dicts of job data scraped THIS session
    processed_job_ids_this_session = set() # Tracks Job IDs added to list THIS session
    total_unique_jobs_added_session = 0 # Counter for Proposal #6
    total_added_from_excel_func = 0 # Counter for Proposal #5 summary
    total_skipped_from_excel_func = 0 # Counter for Proposal #5 summary

    base_search_url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(search_query)}"
    if location_text: base_search_url += f"&location={quote_plus(location_text)}"
    if geo_id: base_search_url += f"&geoId={geo_id}"
    base_search_url += "&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true"
    if date_filter_url_param: base_search_url += f"&f_TPR={date_filter_url_param}"
    logging.info(f"Base Search URL: {base_search_url}")

    current_page_number = 1
    try:
        page_url = f"{base_search_url}&start=0"
        logging.info(f"Navigating to initial search URL (Page 1): {page_url}")
        driver.get(page_url)
        logging.info(f"Waiting for initial page load structure (up to {wait_time_container}s)...")
        time.sleep(get_random_delay(config, "long"))

        # --- Main Scraping Loop ---
        while True:
            logging.info(f"--- Processing Page {current_page_number} ---")

            # Check Stop Conditions (BEFORE scraping page) - Proposal #6 logic integrated
            # 1. Max pages reached?
            if current_page_number > max_pages:
                logging.info(f"Reached max pages limit ({max_pages}). Stopping.")
                break
            # 2. Minimum unique target met AND total limit reached (if total limit exists)?
            minimum_target_met = total_unique_jobs_added_session >= minimum_unique_jobs_target
            total_limit_applies = total_jobs_limit and total_jobs_limit > 0
            total_limit_reached = total_limit_applies and total_unique_jobs_added_session >= total_jobs_limit
            if minimum_target_met and total_limit_reached:
                 logging.info(f"Met minimum unique target ({minimum_unique_jobs_target}) and reached total jobs limit ({total_jobs_limit}). Stopping.")
                 break
            # 3. Only scraping first page? (Check after first page processed)
            if not scrape_all and current_page_number > 1:
                 logging.info("scrape_all_pages is False. Stopping after first page.")
                 break

            # Validate URL (optional check)
            expected_start_param = f"start={(current_page_number - 1) * jobs_per_linkedin_page}"
            if expected_start_param not in driver.current_url and current_page_number > 1:
                logging.warning(f"Current URL ({driver.current_url}) doesn't contain '{expected_start_param}'. Pagination check.")

            # --- Wait for and Scroll Page Content ---
            jobs_list_container = None
            try:
                logging.info(f"Waiting for job list container ('{selectors['job_list_container']}')...")
                jobs_list_container = WebDriverWait(driver, wait_time_container).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_list_container'])))
                logging.info(f"Waiting for job cards ('{selectors['job_card']}') within container...")
                WebDriverWait(jobs_list_container, wait_time_cards).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_card'])))
                logging.info(f"Initial cards present on page {current_page_number}.")
                # Scrolling logic (remains the same)
                logging.info("Performing scrolls...")
                scroll_increment = 800; scroll_pauses = 3; current_scroll = 0
                page_height = driver.execute_script("return document.body.scrollHeight")
                for _ in range(scroll_pauses):
                     current_scroll += scroll_increment; current_scroll = min(current_scroll, page_height)
                     driver.execute_script(f"window.scrollTo(0, {current_scroll});")
                     time.sleep(get_random_delay(config, "medium"))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(get_random_delay(config, "medium"))
                logging.info("Scroll attempts finished.")
            except TimeoutException:
                logging.warning(f"Timeout waiting for container or cards on page {current_page_number}.")
                if current_page_number == 1:
                    try: driver.find_element(By.XPATH, selectors['no_results_banner']); logging.info("'No matching jobs found'. Stopping."); break
                    except NoSuchElementException: logging.error("Timeout on first page, no 'No matching jobs' message."); break
                else: logging.info("Timeout on subsequent page, assuming end of results."); break

            # --- Extract Data ---
            logging.info(f"Extracting job data from page {current_page_number}...")
            job_cards = []
            try:
                jobs_list_container = driver.find_element(By.CSS_SELECTOR, selectors['job_list_container'])
                job_cards = jobs_list_container.find_elements(By.CSS_SELECTOR, selectors['job_card'])
                logging.info(f"Found {len(job_cards)} job card elements after scrolling.")
            except Exception as e: logging.error(f"Error finding card elements after scroll: {e}"); break
            if not job_cards: logging.warning("No card elements found after scroll. Stopping."); break

            page_extracted_count = 0
            current_page_jobs_data = [] # Store jobs scraped from THIS page

            for i, card in enumerate(job_cards):
                # Check Limits before processing card (respects total limit primarily for efficiency)
                if total_limit_applies and total_unique_jobs_added_session >= total_jobs_limit:
                    # Only break if min target is also met (otherwise continue to find unique)
                    if minimum_target_met:
                        logging.info(f"Reached total jobs limit ({total_jobs_limit}) after meeting minimum target. Stopping card processing for page.")
                        break
                if jobs_per_page_limit and page_extracted_count >= jobs_per_page_limit:
                    logging.info(f"Reached jobs/page limit ({jobs_per_page_limit}).")
                    break

                logging.debug(f"Processing card {i+1}/{len(job_cards)}...")
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", card)
                    time.sleep(0.6)
                    try: WebDriverWait(card, wait_time_element).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_card_link'])))
                    except TimeoutException: logging.warning(f"Card {i+1}: Timed out waiting for link after scroll. Skipping."); continue

                    job_data = extract_job_data_from_card(card, config)

                    if job_data:
                        current_job_id = job_data.get('Job ID', 'N/A')
                        # Check uniqueness within THIS SESSION only here
                        if current_job_id != 'N/A' and current_job_id not in processed_job_ids_this_session:
                            processed_job_ids_this_session.add(current_job_id)
                            current_page_jobs_data.append(job_data) # Add to page list
                            page_extracted_count += 1
                            # Note: total_unique_jobs_added_session incremented after Excel check
                        elif current_job_id != 'N/A':
                             logging.debug(f"Job ID {current_job_id} already processed this session. Skipping card.")
                        else: logging.warning(f"Card {i+1} extracted but has Job ID 'N/A'. Skipping.")
                    else: logging.warning(f"Card {i+1} did not yield valid data.")

                except StaleElementReferenceException: logging.warning(f"Card {i+1} stale. Skipping."); time.sleep(0.5); continue
                except ElementNotInteractableException: logging.warning(f"Card {i+1} not interactable. Skipping."); continue
                except Exception as card_err: logging.error(f"Error processing card {i+1}: {card_err}", exc_info=True)

            # MODIFIED (Proposal #3): Update log message
            logging.info(f"Extracted {page_extracted_count} job cards from page {current_page_number} (pre-deduplication).")

            # Add unique jobs from THIS page to the overall list
            all_scraped_jobs_data.extend(current_page_jobs_data)
            # Update the session unique count based on jobs added to the list
            total_unique_jobs_added_session = len(processed_job_ids_this_session)
            logging.info(f"Session Total Unique Jobs Found So Far: {total_unique_jobs_added_session}")


            if save_each_page and page_extracted_count > 0:
                 logging.info(f"Saving intermediate results for page {current_page_number}...")
                 # MODIFIED (Proposal #5): Capture counts from add_jobs_to_excel
                 success_save, added_count, skipped_count = add_jobs_to_excel(current_page_jobs_data, config)
                 if success_save:
                     total_added_from_excel_func += added_count
                     total_skipped_from_excel_func += skipped_count
                     logging.info(f"Intermediate save OK (Added: {added_count}, Skipped Duplicates: {skipped_count}).")
                 else:
                     logging.error("Failed intermediate save. Stopping phase."); return [], 0, 0 # Critical failure

            # Check if we should stop pagination based on lack of new jobs (and scrape_all)
            if page_extracted_count == 0 and scrape_all and current_page_number > 1:
                logging.info(f"No new job cards extracted from page {current_page_number}. Assuming end of results.")
                break

            # --- Pagination Logic ---
            # (Remains the same: URL first, button fallback)
            current_page_number += 1
            next_start_index = (current_page_number - 1) * jobs_per_linkedin_page
            next_page_url = f"{base_search_url}&start={next_start_index}"
            logging.info(f"Attempting pagination to Page {current_page_number} using URL...")
            try:
                driver.get(next_page_url)
                WebDriverWait(driver, wait_time_container).until(EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_list_container'])))
                logging.info(f"Page {current_page_number} loaded successfully via URL.")
                time.sleep(get_random_delay(config, "long"))
            except Exception as url_nav_err:
                logging.warning(f"Navigation via URL to page {current_page_number} failed: {url_nav_err}")
                logging.info(f"Attempting pagination to Page {current_page_number} using Button Click fallback...")
                current_page_number -= 1
                next_page_number_to_click = current_page_number + 1
                try:
                    pagination_controls = driver.find_element(By.CSS_SELECTOR, selectors['pagination_container'])
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", pagination_controls)
                    time.sleep(get_random_delay(config, "short"))
                    next_page_button_selector = selectors['pagination_button_template'].format(next_page_number_to_click)
                    next_button = WebDriverWait(driver, selenium_cfg['wait_time_short']).until(EC.element_to_be_clickable((By.CSS_SELECTOR, next_page_button_selector)))
                    driver.execute_script("arguments[0].click();", next_button)
                    current_page_number = next_page_number_to_click
                    logging.info(f"Waiting for page {current_page_number} to load (Fallback)...")
                    time.sleep(get_random_delay(config, "long"))
                except Exception as page_err: logging.error(f"Fallback Button Click failed: {page_err}. Assuming end."); break
        # --- End Main Loop ---

    except WebDriverException as e: logging.critical(f"WebDriverException during scraping: {e}", exc_info=True); raise e # Re-raise critical
    except Exception as e: logging.error(f"Unexpected error during job search/scraping: {e}", exc_info=True); return [], 0, 0 # Return empty list and zero counts

    # Final Excel save if not saving each page
    if not save_each_page and all_scraped_jobs_data:
         logging.info(f"Performing final save of {len(all_scraped_jobs_data)} scraped jobs...")
         success_final, added_final, skipped_final = add_jobs_to_excel(all_scraped_jobs_data, config)
         if success_final:
             total_added_from_excel_func = added_final
             total_skipped_from_excel_func = skipped_final
             logging.info(f"Final save complete (Added: {added_final}, Skipped Duplicates: {skipped_final}).")
         else:
             logging.error("CRITICAL: Final Excel save failed.")
             return [], 0, 0 # Indicate critical failure

    logging.info(f"Finished scraping job list. Total unique jobs found this session: {total_unique_jobs_added_session}")
    # MODIFIED (Proposal #5): Return tracked counts
    return all_scraped_jobs_data, total_added_from_excel_func, total_skipped_from_excel_func

# MODIFIED (Proposals #4, #5): Remove individual duplicate log, return counts
def add_jobs_to_excel(scraped_jobs_list, config):
    """Adds scraped job data to Excel, returns (success_bool, added_count, skipped_duplicates)."""
    excel_filepath = config['paths']['excel_filepath']
    new_status = config['status']['NEW']
    logging.info(f"Processing {len(scraped_jobs_list)} scraped jobs for Excel file: {excel_filepath}")

    added_count = 0
    skipped_duplicates = 0
    new_jobs_df = None

    if scraped_jobs_list:
        try:
            new_jobs_df = pd.DataFrame(scraped_jobs_list)
            if new_jobs_df.empty: logging.info("DataFrame from scraped jobs is empty.")
            else:
                logging.info(f"Created DataFrame with {len(new_jobs_df)} new jobs from this batch.")
                new_jobs_df['Date Added'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                new_jobs_df['Status'] = new_status
                # Ensure all expected columns exist (using the updated list implicitly)
                for col in ALL_EXPECTED_COLUMNS:
                    if col not in new_jobs_df.columns: new_jobs_df[col] = pd.NA # Initialize missing as NA/None
                # Create unique key
                if all(k in new_jobs_df.columns for k in ['Link', 'Title', 'Company']):
                     new_jobs_df['unique_key'] = (new_jobs_df['Link'].astype(str).str.lower() + '|' +
                                                  new_jobs_df['Title'].astype(str).str.lower() + '|' +
                                                  new_jobs_df['Company'].astype(str).str.lower())
                else: logging.error("Missing key columns for deduplication."); new_jobs_df['unique_key'] = None
        except Exception as df_err: logging.error(f"Error processing DataFrame from scraped jobs: {df_err}", exc_info=True); return False, 0, 0
    else: logging.info("No new jobs scraped in this batch to add.")

    # --- Read Existing Data and Merge ---
    file_exists = os.path.exists(excel_filepath)
    df_to_save = None
    save_needed = False

    try:
        existing_df = pd.DataFrame(columns=ALL_EXPECTED_COLUMNS)
        existing_keys = set()
        if file_exists:
            logging.info(f"Reading existing Excel file: {excel_filepath}")
            try:
                existing_df = pd.read_excel(excel_filepath, engine='openpyxl', dtype=object).fillna('')
                logging.info(f"Read {len(existing_df)} existing records.")
                # --- Schema Check & Update for EXISTING DataFrame ---
                added_missing_cols_existing = False
                for col in ALL_EXPECTED_COLUMNS:
                    if col not in existing_df.columns:
                        logging.warning(f"Adding missing column '{col}' to existing DataFrame.")
                        existing_df[col] = ''
                        added_missing_cols_existing = True
                if added_missing_cols_existing:
                     logging.info("Reordering columns in existing DataFrame.")
                     existing_df = existing_df.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='')
                     save_needed = True
                # --- Create Existing Keys ---
                if all(k in existing_df.columns for k in ['Link', 'Title', 'Company']):
                    existing_df['unique_key'] = (existing_df['Link'].astype(str).str.lower() + '|' +
                                                 existing_df['Title'].astype(str).str.lower() + '|' +
                                                 existing_df['Company'].astype(str).str.lower())
                    existing_keys = set(existing_df['unique_key'])
                    existing_df = existing_df.drop(columns=['unique_key'])
                else: logging.warning("Missing key columns in existing data. Deduplication skipped.")
            except zipfile.BadZipFile:
                logging.warning(f"Existing Excel file '{excel_filepath.name}' is corrupt or invalid. Treating as empty and overwriting.")
                existing_df = pd.DataFrame(columns=ALL_EXPECTED_COLUMNS) # Start fresh
                existing_keys = set()
                save_needed = True # Force save to create a valid file structure
            except Exception as read_err: logging.error(f"Error reading/processing existing Excel: {read_err}", exc_info=True); return False, added_count, skipped_duplicates

        # --- Process New Jobs (Deduplication) ---
        unique_new_jobs_list = []
        if new_jobs_df is not None and not new_jobs_df.empty and 'unique_key' in new_jobs_df.columns:
            logging.info("Checking scraped jobs against existing data...")
            for index, row in new_jobs_df.iterrows():
                key = row['unique_key']
                if key is None or key in existing_keys:
                    skipped_duplicates += 1
                    # MODIFIED (Proposal #4): Removed individual duplicate log
                    # logging.info(f"  Skipped Duplicate: '{row.get('Title', 'N/A')}' @ '{row.get('Company', 'N/A')}'")
                else:
                    unique_new_jobs_list.append(row.drop('unique_key').to_dict())
                    existing_keys.add(key)
            added_count = len(unique_new_jobs_list)
            logging.info(f"Deduplication complete: {added_count} new unique jobs to add, {skipped_duplicates} duplicates skipped.")
            if added_count > 0:
                new_unique_df = pd.DataFrame(unique_new_jobs_list).reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='')
                df_to_save = pd.concat([existing_df, new_unique_df], ignore_index=True)
                save_needed = True
            else: df_to_save = existing_df
        else: df_to_save = existing_df

        # --- Create File if Doesn't Exist ---
        if not file_exists and not scraped_jobs_list:
             logging.info(f"Creating new Excel file with headers: {excel_filepath}")
             df_to_save = pd.DataFrame(columns=ALL_EXPECTED_COLUMNS)
             save_needed = True

        # --- Save to Excel ---
        if save_needed and df_to_save is not None:
            logging.info(f"Attempting to save DataFrame ({len(df_to_save)} rows) to Excel...")
            df_to_save = df_to_save.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='') # Final order/fill
            # Convert potential pandas NA to empty strings for Excel compatibility if preferred
            df_to_save = df_to_save.fillna('')
            df_to_save.to_excel(excel_filepath, index=False, engine='openpyxl')
            logging.info(f"Successfully saved Excel file: {excel_filepath}")
        else: logging.info("No changes needed for the Excel file in this batch.")

        # MODIFIED (Proposal #5): Return counts
        return True, added_count, skipped_duplicates

    except PermissionError: logging.error(f"PERM ERROR writing to Excel: {excel_filepath}."); return False, added_count, skipped_duplicates
    except Exception as e: logging.error(f"Unexpected error during Excel processing: {e}", exc_info=True); return False, added_count, skipped_duplicates

# MODIFIED (Proposal #5): Capture and log summary counts
def run_phase1_job_list_scraping(config):
    """Executes Phase 1, connects Selenium, scrapes, adds to Excel, returns summary."""
    logging.info("Initiating Phase 1: Job List Scraping")
    driver = None
    overall_success = False
    total_added_session = 0
    total_skipped_session = 0

    try:
        driver = setup_selenium_driver(config)
        if not driver:
            logging.critical("Failed to setup Selenium WebDriver. Phase 1 cannot proceed.")
            return False, 0, 0 # Return False and zero counts

        # MODIFIED: Capture returned counts
        scraped_jobs, total_added_session, total_skipped_session = search_and_scrape_jobs(driver, config)

        # Excel handling logic moved inside search_and_scrape_jobs for final save if needed
        # Check if scraping itself was successful (even if no jobs added)
        # The search function might return counts even if excel write failed intermediately,
        # so we rely on its return value for overall success.
        # A critical Excel error within search_and_scrape would likely return ([], 0, 0) or raise.
        if isinstance(scraped_jobs, list): # Check if scraping function executed without critical error
            overall_success = True # Mark as successful run, even if 0 jobs added
            if not scraped_jobs and total_added_session == 0 and total_skipped_session == 0:
                 logging.info("No jobs were scraped or added in this session.")
                 # Ensure excel file exists with headers if no jobs found
                 logging.info("Ensuring Excel file structure exists...")
                 excel_init_success, _, _ = add_jobs_to_excel([], config)
                 if not excel_init_success:
                     logging.error("Failed during Excel initialization check even with no new jobs.")
                     overall_success = False # Critical if we can't create/verify the file

        else: # Should not happen if search_and_scrape returns correctly
            logging.error("Scraping function did not return expected list.")
            overall_success = False

    except WebDriverException as e:
         logging.critical(f"WebDriverException during Phase 1 execution: {e}", exc_info=True)
         overall_success = False
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in Phase 1: {e}", exc_info=True)
        overall_success = False

    # MODIFIED (Proposal #5): Add summary log before final completion message
    if overall_success:
         logging.info(f"Phase 1 Summary: Added {total_added_session} unique jobs to Excel. Skipped {total_skipped_session} duplicates found during processing.")
         logging.info("Phase 1 completed successfully.")
    else:
         logging.error("Phase 1 finished with errors.")

    # MODIFIED (Proposal #5): Return success status and counts
    return overall_success, total_added_session, total_skipped_session