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

# Define ALL columns expected in the final Excel sheet across all phases
# Ensures the initial file has the complete structure.
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

def setup_selenium_driver(config):
    """Connects Selenium to an existing Chrome instance using config."""
    driver_path = config['selenium']['chromedriver_path']
    port = config['selenium']['debugger_port']
    logging.info(f"Attempting to connect to existing Chrome on port {port}...")
    logging.info(f"Using ChromeDriver path: {driver_path}")

    if not os.path.exists(driver_path):
        logging.error(f"ChromeDriver executable not found at: {driver_path}")
        return None

    service = Service(executable_path=driver_path)
    options = webdriver.ChromeOptions()
    options.add_experimental_option("debuggerAddress", f"localhost:{port}")

    # --- Alternative ways to potentially reduce logging (Use if needed, but often not necessary) ---
    # options.add_argument("--log-level=3") # Sets minimal logging level for Chrome itself
    # options.add_argument("--silent")      # Can sometimes help reduce ChromeDriver output
    # --------------------------------------------------------------------------------------------

    try:
        driver = webdriver.Chrome(service=service, options=options)
        logging.info("Successfully connected to Chrome debugger.")
        # Basic check
        time.sleep(0.5)
        if not driver.current_url:
            logging.warning("Connected, but could not get current URL. Browser might be stuck.")
        elif "linkedin.com" not in driver.current_url:
            logging.warning(f"Connected, but the active tab is not LinkedIn ({driver.current_url}). Ensure LinkedIn is open and active.")
        else:
            logging.info(f"Confirmed connection on a LinkedIn page: {driver.current_url}")
        return driver
    except WebDriverException as e:
        # Improved error message for this specific connection type
        if "cannot parse capability" in str(e) or "unrecognized chrome option" in str(e):
             logging.error(f"WebDriverException connecting to Chrome (Capability/Option Error). Check Chrome/ChromeDriver compatibility and options. Error: {e}")
        elif "failed to connect" in str(e) or "timed out" in str(e):
             logging.error(f"WebDriverException connecting to Chrome on port {port}. Is Chrome running with --remote-debugging-port={port}? Is the port correct? Error: {e}")
        else:
             logging.error(f"WebDriverException connecting to Chrome: {e}")
        # Log less verbose traceback for known connection issues unless debugging is needed
        # logging.error(traceback.format_exc())
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during Selenium setup: {e}")
        logging.error(traceback.format_exc())
        return None

def parse_posted_ago(text):
    """Parses 'Posted Ago' text into days and hours."""
    if not isinstance(text, str):
        return -1, -1
    text_lower = text.lower()
    days = -1
    hours = -1

    if "just now" in text_lower:
        days = 0
        hours = 0
    elif "yesterday" in text_lower:
        days = 1
        hours = 0
    else:
        # Regex to find number and unit (hour, day, week, month, year)
        match = re.search(r'(\d+)\s+(hour|day|week|month|year)s?', text_lower)
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            if unit == 'hour':
                hours = num
                days = hours // 24
                hours %= 24
            elif unit == 'day':
                days = num
                hours = 0
            elif unit == 'week':
                days = num * 7
                hours = 0
            elif unit == 'month':
                days = num * 30 # Approximation
            elif unit == 'year':
                days = num * 365 # Approximation
            else: # Should not happen with the regex, but safety first
                days = -1
                hours = -1
                logging.warning(f"Unexpected unit '{unit}' in parse_posted_ago for text: {text}")

    # Logging the parsed result can be useful for debugging date logic
    # logging.debug(f"Parsed '{text}' -> Days: {days}, Hours: {hours}")
    return days, hours

def extract_job_data_from_card(card_element, config):
    """Extracts detailed data points from a single job card element using selectors from config."""
    selectors = config['selectors']
    verbose = config['phase1']['verbose_card_extraction']
    if verbose: logging.debug("Processing card...")

    data = { # Initialize with defaults
        'Job ID': 'N/A', 'Title': 'N/A', 'Company': 'N/A', 'Location': 'N/A',
        'Workplace Type': 'N/A', 'Link': 'N/A', 'Easy Apply': False, 'Promoted': False,
        'Viewed': False, 'Early Applicant': False, 'Verified': False,
        'Posted Ago Text': 'N/A', 'Posted Days Ago': -1, 'Posted Hours Ago': -1,
        'Salary Range': 'N/A', 'Insights': 'N/A', 'Company Logo URL': 'N/A',
        'Source': 'LinkedIn Job Search' # Hardcoded source for this phase
    }

    # --- Essential Info: Link, Title, Job ID ---
    try:
        link_element = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_link'])
        data['Link'] = link_element.get_attribute('href')
        if data['Link'] and '?' in data['Link']:
            data['Link'] = data['Link'].split('?')[0] # Clean URL params

        # Extract Title (Try strong tag first, then fallback)
        try:
            data['Title'] = link_element.find_element(By.CSS_SELECTOR, selectors['job_card_title']).text.strip()
        except NoSuchElementException:
            data['Title'] = link_element.text.strip() # Fallback to link text

        # Extract Job ID (often on the parent element)
        try:
            # Go up one level from the link to the container usually holding the ID
            parent_li = link_element.find_element(By.XPATH, './ancestor::li')
            data['Job ID'] = parent_li.get_attribute('data-occludable-job-id') or parent_li.get_attribute('data-entity-urn') # Try common attributes
            if data['Job ID'] and 'jobPosting:' in data['Job ID']:
                data['Job ID'] = data['Job ID'].split(':')[-1] # Extract numeric ID
        except Exception:
            logging.debug("Could not find Job ID attribute via common methods.", exc_info=True)
            data['Job ID'] = 'N/A' # Ensure it's marked N/A if extraction failed

        if not data['Link'] or data['Link'] == 'N/A' or not data['Title'] or data['Title'] == 'N/A':
             logging.warning(f"Card skipped: Missing essential Link ('{data['Link']}') or Title ('{data['Title']}').")
             return None

        if verbose: logging.debug(f"  Found Link: {data['Link']}, Title: {data['Title']}, JobID: {data['Job ID']}")

    except NoSuchElementException:
        logging.warning(f"Card skipped: Essential selector '{selectors['job_card_link']}' failed.")
        return None
    except Exception as e:
        logging.error(f"Card skipped: Unexpected error getting essential info: {e}", exc_info=True)
        return None

    # --- Other Fields (Robust extraction with individual try-except) ---
    try:
        data['Company'] = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_company']).text.strip()
    except Exception: data['Company'] = 'N/A (Company)' # Indicate failed field

    try:
        location_element = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_location'])
        job_location_raw = location_element.text.strip()
        # Extract Workplace Type (On-site, Remote, Hybrid) if present
        match = re.search(r'\((On-site|Remote|Hybrid)\)', job_location_raw, re.IGNORECASE)
        if match:
            data['Workplace Type'] = match.group(1).capitalize()
            data['Location'] = job_location_raw.replace(match.group(0), '').strip(' ·,') # Remove type from location
        else:
            data['Location'] = job_location_raw
        # Remove company name if it appears in location string
        if data['Company'] != 'N/A (Company)' and data['Company'] in data['Location']:
            data['Location'] = data['Location'].replace(data['Company'], '').strip(' ·,')
    except Exception: data['Location'] = 'N/A (Location)'

    try: # Verified Badge Check
        card_element.find_element(By.CSS_SELECTOR, 'svg[data-test-icon="verified-small"]')
        data['Verified'] = True
    except NoSuchElementException: data['Verified'] = False
    except Exception: data['Verified'] = False # Error implies not verified

    try: # Logo
        logo_img = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_logo'])
        data['Company Logo URL'] = logo_img.get_attribute('src')
    except Exception: data['Company Logo URL'] = 'N/A'

    try: # Footer Info (Easy Apply, Posted Ago, etc.)
        footer_element = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_footer_list'])
        footer_items = footer_element.find_elements(By.TAG_NAME, 'li')
        for item in footer_items:
            item_text = item.text # Get text once
            item_text_lower = item_text.lower() # Lowercase for checks
            if "easy apply" in item_text_lower: data['Easy Apply'] = True
            if "viewed" in item_text_lower: data['Viewed'] = True
            if "promoted" in item_text_lower: data['Promoted'] = True
            if "early applicant" in item_text_lower: data['Early Applicant'] = True
            # Find Posted Ago text (check time tag first, then common keywords)
            try:
                time_tag = item.find_element(By.TAG_NAME, 'time')
                data['Posted Ago Text'] = time_tag.text.strip()
            except NoSuchElementException:
                 if any(word in item_text_lower for word in ["ago", "hour", "day", "week", "month", "yesterday", "just now"]):
                     # Use the full text of the list item if it seems like a date string
                     data['Posted Ago Text'] = item_text.strip()

        # Parse the found text after checking all items
        if data['Posted Ago Text'] != 'N/A':
            data['Posted Days Ago'], data['Posted Hours Ago'] = parse_posted_ago(data['Posted Ago Text'])

    except NoSuchElementException:
         if verbose: logging.debug("  Footer list not found.")
    except Exception as footer_err:
         logging.warning(f"  Error parsing footer info: {footer_err}")

    try: # Salary
        data['Salary Range'] = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_salary']).text.strip()
    except NoSuchElementException: data['Salary Range'] = 'N/A'
    except Exception: data['Salary Range'] = 'N/A'

    try: # Insights
        data['Insights'] = card_element.find_element(By.CSS_SELECTOR, selectors['job_card_insights']).text.strip()
    except NoSuchElementException: data['Insights'] = 'N/A'
    except Exception: data['Insights'] = 'N/A'

    if verbose: logging.debug(f"  Extracted Data: {data}")
    return data

# **** START REPLACEMENT for search_and_scrape_jobs function in phase1_list_scraper.py ****
def search_and_scrape_jobs(driver: webdriver, config: dict) -> list:
    """Searches jobs, handles pagination (URL preferred, button fallback), scrapes results, explicit waits."""
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
    wait_time_element = selenium_cfg.get('wait_time_element', 3) # Shorter wait for specific element within card
    jobs_per_page_limit = phase1_cfg.get('jobs_per_page_limit', 0) or None
    total_jobs_limit = phase1_cfg.get('total_jobs_limit', 0) or None
    jobs_per_linkedin_page = 25 # LinkedIn typically shows 25 jobs per page/start increment

    date_filter_map = {'1': None, '2': 'r2592000', '3': 'r604800', '4': 'r86400'}
    date_filter_url_param = date_filter_map.get(date_filter_choice)

    logging.info("Starting job search...")
    logging.info(f"Query='{search_query}', Location='{location_text}', GeoID='{geo_id}', DateFilter='{date_filter_url_param}'")
    logging.info(f"ScrapeAll={scrape_all}, MaxPages={max_pages}, JobsPerPageLimit={jobs_per_page_limit}, TotalJobsLimit={total_jobs_limit}")

    all_scraped_jobs = []
    processed_job_ids_this_session = set()
    total_unique_jobs_added_session = 0

    # Construct Base URL (without start param initially)
    base_search_url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(search_query)}"
    if location_text: base_search_url += f"&location={quote_plus(location_text)}"
    if geo_id: base_search_url += f"&geoId={geo_id}"
    base_search_url += "&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true"
    if date_filter_url_param: base_search_url += f"&f_TPR={date_filter_url_param}"

    logging.info(f"Base Search URL: {base_search_url}")

    current_page_number = 1
    try:
        # --- Initial Navigation (Page 1 / start=0) ---
        page_url = f"{base_search_url}&start=0"
        logging.info(f"Navigating to initial search URL (Page 1): {page_url}")
        driver.get(page_url)
        logging.info(f"Waiting for initial page load structure (up to {wait_time_container}s)...")
        time.sleep(get_random_delay(config, "long"))

        # --- Main Scraping Loop ---
        while True:
            if total_jobs_limit and total_unique_jobs_added_session >= total_jobs_limit: logging.info(f"Reached total jobs limit ({total_jobs_limit}). Stopping."); break
            if scrape_all and current_page_number > max_pages: logging.info(f"Reached max pages limit ({max_pages}). Stopping."); break

            logging.info(f"--- Processing Page {current_page_number} ---")
            # Validate we are on the correct page (optional check)
            expected_start_param = f"start={(current_page_number - 1) * jobs_per_linkedin_page}"
            if expected_start_param not in driver.current_url and current_page_number > 1:
                logging.warning(f"Current URL ({driver.current_url}) doesn't contain expected '{expected_start_param}'. Pagination might be stuck.")
                # Potentially break or try button click here if URL method failed

            jobs_list_container = None
            try:
                # Wait for container and initial cards
                logging.info(f"Waiting for job list container ('{selectors['job_list_container']}')...")
                jobs_list_container = WebDriverWait(driver, wait_time_container).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_list_container']))
                )
                logging.debug("Job list container found.")
                logging.info(f"Waiting for job cards ('{selectors['job_card']}') within container...")
                WebDriverWait(jobs_list_container, wait_time_cards).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_card']))
                )
                logging.info(f"Initial cards present on page {current_page_number}.")

                # --- Scrolling --- (Keep this for lazy loading)
                logging.info("Performing scrolls...")
                scroll_increment = 800; scroll_pauses = 3; current_scroll = 0
                page_height = driver.execute_script("return document.body.scrollHeight")
                for _ in range(scroll_pauses):
                     current_scroll += scroll_increment
                     if current_scroll > page_height: current_scroll = page_height
                     driver.execute_script(f"window.scrollTo(0, {current_scroll});")
                     logging.debug(f" Scrolled to ~{current_scroll}px")
                     time.sleep(get_random_delay(config, "medium"))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(get_random_delay(config, "medium"))
                logging.info("Scroll attempts finished.")

            except TimeoutException:
                # (Keep existing timeout handling logic)
                logging.warning(f"Timeout waiting for container or cards on page {current_page_number}.")
                if current_page_number == 1:
                    try:
                        driver.find_element(By.XPATH, selectors['no_results_banner'])
                        logging.info("'No matching jobs found' message detected. Stopping.")
                        break
                    except NoSuchElementException: logging.error("Timeout on first page, no 'No matching jobs' message."); break
                    except Exception as e: logging.error(f"Error checking 'No matching jobs': {e}"); break
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
            for i, card in enumerate(job_cards):
                # Check Limits
                if jobs_per_page_limit and page_extracted_count >= jobs_per_page_limit: logging.info(f"Reached jobs/page limit ({jobs_per_page_limit})."); break
                if total_jobs_limit and total_unique_jobs_added_session >= total_jobs_limit: logging.info(f"Reached total jobs limit ({total_jobs_limit})."); total_unique_jobs_added_session = total_jobs_limit + 1; break

                logging.debug(f"Processing card {i+1}/{len(job_cards)}...")
                try:
                    # 1. Scroll card into view
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", card)
                    time.sleep(0.6) # Wait for scroll animation

                    # --- NEW: Explicit Wait for Link Element within Card ---
                    try:
                        WebDriverWait(card, wait_time_element).until(
                           EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_card_link']))
                        )
                        logging.debug(f" Essential link found within card {i+1}.")
                    except TimeoutException:
                         logging.warning(f"Card {i+1}: Timed out waiting for essential link element ('{selectors['job_card_link']}') AFTER scrolling into view. Skipping card.")
                         continue # Skip this card if the link doesn't appear
                    # --- END NEW ---

                    # 2. Extract data (now link should be present)
                    job_data = extract_job_data_from_card(card, config)

                    if job_data:
                        current_job_id = job_data.get('Job ID', 'N/A')
                        if current_job_id != 'N/A' and current_job_id not in processed_job_ids_this_session:
                            processed_job_ids_this_session.add(current_job_id)
                            all_scraped_jobs.append(job_data)
                            page_extracted_count += 1
                            total_unique_jobs_added_session += 1
                        elif current_job_id == 'N/A': logging.warning(f"Card {i+1} extracted but has Job ID 'N/A'. Skipping.")
                        else: logging.debug(f"Job ID {current_job_id} processed this session. Skipping.")
                    else:
                        # extract_job_data_from_card already logged the skip reason if link failed initially
                        if 'Essential selector' not in str(logging.getLogger().handlers[0].stream): # Avoid double logging if already logged in extractor
                           logging.warning(f"Card {i+1} did not yield valid data (possible structure mismatch inside?).")

                except StaleElementReferenceException: logging.warning(f"Card {i+1} stale. Skipping."); time.sleep(0.5); continue
                except ElementNotInteractableException: logging.warning(f"Card {i+1} not interactable. Skipping."); continue
                except Exception as card_err: logging.error(f"Error processing card {i+1}: {card_err}", exc_info=True)

            logging.info(f"Extracted {page_extracted_count} new unique jobs from page {current_page_number}.")

            if save_each_page and page_extracted_count > 0:
                 logging.info(f"Saving intermediate results...")
                 if add_jobs_to_excel(all_scraped_jobs, config): logging.info("Intermediate save OK.")
                 else: logging.error("Failed intermediate save."); # Optionally stop: return False

            # Check limits again before pagination
            if total_jobs_limit and total_unique_jobs_added_session >= total_jobs_limit: logging.info(f"Reached total jobs limit ({total_jobs_limit}). Stopping."); break
            if not scrape_all: logging.info("scrape_all_pages is False. Stopping."); break
            if page_extracted_count == 0 and current_page_number > 1: logging.info("No new jobs extracted. Assuming end."); break

            # --- Pagination: URL First, Button Fallback ---
            current_page_number += 1
            next_start_index = (current_page_number - 1) * jobs_per_linkedin_page
            next_page_url = f"{base_search_url}&start={next_start_index}"
            logging.info(f"Attempting pagination to Page {current_page_number} using URL: {next_page_url}")

            try:
                driver.get(next_page_url)
                logging.info(f"Waiting for Page {current_page_number} content to load via URL...")
                # Add a check: Wait for the container to appear on the new page
                WebDriverWait(driver, wait_time_container).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selectors['job_list_container']))
                )
                logging.info(f"Page {current_page_number} loaded successfully via URL.")
                time.sleep(get_random_delay(config, "long")) # Wait after load

            except Exception as url_nav_err:
                logging.warning(f"Navigation via URL to page {current_page_number} failed: {url_nav_err}")
                logging.info(f"Attempting pagination to Page {current_page_number} using Button Click as fallback...")
                # Reset page number as URL navigation failed
                current_page_number -= 1
                next_page_number_to_click = current_page_number + 1

                try:
                    pagination_controls = driver.find_element(By.CSS_SELECTOR, selectors['pagination_container'])
                    # Scroll pagination into view just in case
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", pagination_controls)
                    time.sleep(get_random_delay(config, "short"))

                    next_page_button_selector = selectors['pagination_button_template'].format(next_page_number_to_click)
                    logging.debug(f"Looking for fallback pagination button: '{next_page_button_selector}'")

                    next_button = WebDriverWait(driver, selenium_cfg['wait_time_short']).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, next_page_button_selector))
                    )
                    logging.info(f"Found page {next_page_number_to_click} button (Fallback). Clicking...")
                    driver.execute_script("arguments[0].click();", next_button)
                    current_page_number = next_page_number_to_click # Update page number on successful click
                    logging.info(f"Waiting for page {current_page_number} to load (Fallback)...")
                    time.sleep(get_random_delay(config, "long"))

                except TimeoutException: logging.info(f"Fallback Button: Page {next_page_number_to_click} button timeout. Assuming end."); break
                except ElementNotInteractableException: logging.warning(f"Fallback Button: Page {next_page_number_to_click} not interactable. Assuming end."); break
                except Exception as page_err: logging.error(f"Fallback Button: Error clicking page {next_page_number_to_click}: {page_err}", exc_info=True); break

        # --- End Main Loop ---

    except WebDriverException as e: logging.critical(f"WebDriverException during scraping: {e}", exc_info=True); raise e
    except Exception as e: logging.error(f"Unexpected error during job search/scraping: {e}", exc_info=True); return []

    logging.info(f"Finished scraping job list. Total unique jobs found this session: {total_unique_jobs_added_session}")
    return all_scraped_jobs
# **** END REPLACEMENT for search_and_scrape_jobs function in phase1_list_scraper.py ****

# **** START REPLACEMENT for add_jobs_to_excel function in phase1_list_scraper.py ****
def add_jobs_to_excel(scraped_jobs_list, config):
    """Adds scraped job data to Excel, creating/updating file/columns, logging duplicates."""
    excel_filepath = config['paths']['excel_filepath']
    new_status = config['status']['NEW']
    logging.info(f"Processing {len(scraped_jobs_list)} scraped jobs for Excel file: {excel_filepath}")

    new_jobs_df = None # Initialize as None
    if scraped_jobs_list:
        try:
            new_jobs_df = pd.DataFrame(scraped_jobs_list)
            if new_jobs_df.empty:
                logging.info("DataFrame created from scraped jobs is empty.")
            else:
                logging.info(f"Created DataFrame with {len(new_jobs_df)} new jobs from this session.")
                new_jobs_df['Date Added'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                new_jobs_df['Status'] = new_status

                logging.debug("Ensuring all expected columns exist in the new data...")
                for col in ALL_EXPECTED_COLUMNS:
                    if col not in new_jobs_df.columns:
                        logging.debug(f" Adding missing column '{col}' to new DataFrame.")
                        if col in ['Notes', 'Job Description HTML', 'Job Description Plain Text', 'About Company', 'Skills Required', 'Applied Date', 'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills', 'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description', 'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement', 'AI Actionable Recommendations', 'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List', 'Tailored HTML Path', 'Tailored PDF Path', 'Source', 'Title', 'Company', 'Location', 'Workplace Type', 'Link', 'Posted Ago Text', 'Salary Range', 'Insights', 'Company Logo URL', 'Status', 'Applicant Count', 'Posted Ago Text Detailed', 'Company LinkedIn URL', 'Company Industry', 'Company Size', 'Company LinkedIn Members', 'Company Followers', 'Hiring Team Member 1 Name', 'Hiring Team Member 1 Profile URL', 'Hiring Team Member 2 Name', 'Hiring Team Member 2 Profile URL']:
                             new_jobs_df[col] = ''
                        elif col in ['Job ID', 'Posted Days Ago', 'Posted Hours Ago', 'AI Match Score']:
                             new_jobs_df[col] = pd.NA
                        elif col in ['Easy Apply', 'Promoted', 'Viewed', 'Early Applicant', 'Verified']:
                              new_jobs_df[col] = False
                        else:
                             new_jobs_df[col] = pd.NA

                # Create unique key only if there's data and key columns exist
                if all(k in new_jobs_df.columns for k in ['Link', 'Title', 'Company']):
                     new_jobs_df['unique_key'] = (
                         new_jobs_df['Link'].astype(str).str.lower() + '|' +
                         new_jobs_df['Title'].astype(str).str.lower() + '|' +
                         new_jobs_df['Company'].astype(str).str.lower()
                     )
                else:
                     logging.error("Could not create unique key for new jobs - missing Link, Title, or Company column.")
                     new_jobs_df['unique_key'] = None # Handle gracefully

        except Exception as df_err:
             logging.error(f"Error processing DataFrame from scraped jobs: {df_err}", exc_info=True)
             return False
    else:
         logging.info("No new jobs scraped in this session.")

    # --- Read Existing Data and Merge ---
    file_exists = os.path.exists(excel_filepath)
    df_to_save = None
    save_needed = False
    added_count = 0
    skipped_duplicates = 0

    try:
        existing_df = pd.DataFrame(columns=ALL_EXPECTED_COLUMNS)
        existing_keys = set()

        if file_exists:
            logging.info(f"Reading existing Excel file: {excel_filepath}")
            try:
                existing_df = pd.read_excel(excel_filepath, engine='openpyxl', dtype=object).fillna('') # Fillna on read
                logging.info(f"Read {len(existing_df)} existing records.")
                # --- Schema Check & Update for EXISTING DataFrame ---
                added_missing_cols_existing = False
                for col in ALL_EXPECTED_COLUMNS:
                    if col not in existing_df.columns:
                        logging.warning(f"Adding missing column '{col}' to existing DataFrame.")
                        existing_df[col] = '' # Initialize new cols as empty string
                        added_missing_cols_existing = True

                if added_missing_cols_existing:
                     logging.info("Reordering columns in existing DataFrame.")
                     existing_df = existing_df.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='')
                     save_needed = True

                # Ensure key columns exist before creating key
                if all(k in existing_df.columns for k in ['Link', 'Title', 'Company']):
                    existing_df['unique_key'] = (
                        existing_df['Link'].astype(str).str.lower() + '|' +
                        existing_df['Title'].astype(str).str.lower() + '|' +
                        existing_df['Company'].astype(str).str.lower()
                    )
                    existing_keys = set(existing_df['unique_key'])
                    existing_df = existing_df.drop(columns=['unique_key']) # Drop temp key
                else:
                    logging.warning("Could not create unique keys for existing data - missing key columns. Deduplication might be incorrect.")
                    existing_keys = set() # Cannot deduplicate reliably

            except Exception as read_err:
                 logging.error(f"Error reading/processing existing Excel: {read_err}", exc_info=True)
                 existing_df = pd.DataFrame(columns=ALL_EXPECTED_COLUMNS)
                 existing_keys = set()

        # --- Process New Jobs (Deduplication and Logging) ---
        unique_new_jobs_list_of_dicts = [] # Use a list of dicts for cleaner appending
        if new_jobs_df is not None and not new_jobs_df.empty and 'unique_key' in new_jobs_df.columns:
            logging.info("Checking scraped jobs against existing data...")
            for index, row_series in new_jobs_df.iterrows():
                key = row_series['unique_key']
                if key in existing_keys:
                    skipped_duplicates += 1
                    logging.info(f"  Skipped Duplicate: '{row_series.get('Title', 'N/A')}' @ '{row_series.get('Company', 'N/A')}'")
                else:
                    # Convert the row Series (without unique_key) to dict and add to list
                    row_dict = row_series.drop('unique_key').to_dict()
                    unique_new_jobs_list_of_dicts.append(row_dict)
                    existing_keys.add(key) # Add to set to prevent duplicates within new batch

            added_count = len(unique_new_jobs_list_of_dicts)
            logging.info(f"Found {added_count} new unique jobs to add (skipped {skipped_duplicates} duplicates).")

            if added_count > 0:
                # Create DataFrame from the list of unique job dictionaries
                new_unique_df = pd.DataFrame(unique_new_jobs_list_of_dicts)
                # Ensure columns match ALL_EXPECTED_COLUMNS before concat
                new_unique_df = new_unique_df.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='')
                # Combine with existing data
                combined_df = pd.concat([existing_df, new_unique_df], ignore_index=True)
                df_to_save = combined_df
                save_needed = True
            else:
                df_to_save = existing_df # Use existing (might have schema changes)
        else:
            df_to_save = existing_df # No new jobs, use existing

        # --- Create File if Doesn't Exist and No Data ---
        if not file_exists and not scraped_jobs_list: # Check original list, not DataFrame
             logging.info(f"Excel file not found. Creating new file with headers: {excel_filepath}")
             df_to_save = pd.DataFrame(columns=ALL_EXPECTED_COLUMNS)
             save_needed = True

        # --- Save to Excel ---
        if save_needed and df_to_save is not None:
            logging.info(f"Attempting to save DataFrame ({len(df_to_save)} rows) to Excel...")
            df_to_save = df_to_save.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='') # Final order check
            df_to_save.to_excel(excel_filepath, index=False, engine='openpyxl')
            logging.info(f"Successfully saved Excel file: {excel_filepath}")
        elif df_to_save is None and not file_exists:
             logging.error("Logic error: DataFrame to save is None when creating new file.")
             return False
        else:
            logging.info("No changes needed for the Excel file in this run.")

        return True

    except PermissionError: logging.error(f"PERM ERROR writing to Excel: {excel_filepath}."); return False
    except Exception as e: logging.error(f"Unexpected error during Excel processing: {e}", exc_info=True); return False
# **** END REPLACEMENT for add_jobs_to_excel function in phase1_list_scraper.py ****
# --- Main Function for Phase 1 ---
def run_phase1_job_list_scraping(config):
    """
    Executes the Phase 1 workflow: connect to Selenium, scrape job list, add to Excel.

    Args:
        config (dict): The master configuration dictionary.

    Returns:
        bool: True if the phase completed successfully (even if no jobs found),
              False if a critical error occurred (e.g., WebDriver connection failed, Excel write failed).
    """
    logging.info("Initiating Phase 1: Job List Scraping")
    driver = None
    overall_success = False # Assume failure until success steps complete

    try:
        # --- 1. Setup Selenium ---
        driver = setup_selenium_driver(config)
        if not driver:
            # Error already logged in setup_selenium_driver
            logging.critical("Failed to setup Selenium WebDriver. Phase 1 cannot proceed.")
            return False # Critical failure

        # --- 2. Search and Scrape ---
        # search_and_scrape_jobs handles its own logging and non-critical errors
        scraped_jobs = search_and_scrape_jobs(driver, config)

        # --- 3. Add to Excel ---
        if scraped_jobs:
            logging.info(f"Adding {len(scraped_jobs)} scraped jobs to Excel...")
            excel_success = add_jobs_to_excel(scraped_jobs, config)
            if not excel_success:
                logging.error("Failed to add jobs to Excel. Data may be lost.")
                # Decide if this is critical - returning False stops the whole workflow
                return False # Treat Excel write failure as critical
            else:
                overall_success = True # Scraping and writing successful
        else:
            logging.info("No jobs were scraped in this session. Checking/Creating Excel file structure.")
            # Call add_jobs_to_excel with empty list to ensure file/columns exist
            excel_init_success = add_jobs_to_excel([], config)
            if not excel_init_success:
                 logging.error("Failed during Excel initialization/check even with no new jobs.")
                 return False # Critical if we can't even verify the file
            else:
                 overall_success = True # Phase ran successfully, just found no jobs

    except WebDriverException as e:
         # Catch WebDriver errors that might occur outside the scraping loop itself
         logging.critical(f"WebDriverException during Phase 1 execution: {e}")
         logging.critical(traceback.format_exc())
         overall_success = False
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in Phase 1: {e}")
        logging.critical(traceback.format_exc())
        overall_success = False
    # 'finally' block is not strictly needed here as the driver connection is managed
    # by the calling script (or should persist if connected to an existing browser).
    # We don't want to `driver.quit()` if connected via debugger port.

    if overall_success:
         logging.info("Phase 1 completed successfully.")
    else:
         logging.error("Phase 1 finished with errors.")

    return overall_success

# Note: The `if __name__ == "__main__":` block is removed as this script
# is now intended to be imported and run via `main_workflow.py`.