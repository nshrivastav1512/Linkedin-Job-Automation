# phase2_detail_scraper.py
# Phase 2: Scrapes detailed information for individual job links found in Phase 1.

import time
import os
import traceback
import re
import logging
import random
from datetime import datetime
import pandas as pd
from selenium.webdriver.remote.webdriver import WebDriver # Type hinting
from selenium.webdriver.remote.webelement import WebElement # Type hinting
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
    StaleElementReferenceException,
    ElementClickInterceptedException
)
from bs4 import BeautifulSoup

# Import the column list defined in phase1 (or define it consistently)
# Assuming it's accessible or redefined here for clarity/independence if needed.
# from phase1_list_scraper import ALL_EXPECTED_COLUMNS # Option 1: Import
# Option 2: Redefine (ensure it's identical to Phase 1)
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

def safe_get_text(element: WebElement) -> str:
    """Safely get text from a Selenium element, returning empty string on error."""
    try:
        return element.text.strip()
    except (NoSuchElementException, StaleElementReferenceException):
        return ""
    except Exception as e:
        logging.warning(f"Unexpected error getting text from element: {e}", exc_info=False) # Log less verbosely for this helper
        return ""

def safe_get_attribute(element: WebElement, attribute: str) -> str:
    """Safely get an attribute from a Selenium element, returning empty string on error."""
    try:
        return element.get_attribute(attribute) or ""
    except (NoSuchElementException, StaleElementReferenceException):
        return ""
    except Exception as e:
        logging.warning(f"Unexpected error getting attribute '{attribute}' from element: {e}", exc_info=False)
        return ""

def clean_html_for_text(html_content: str) -> str:
    """Uses BeautifulSoup to extract clean plain text from HTML content."""
    if not html_content or pd.isna(html_content):
        return ""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Remove script and style tags first
        for script_or_style in soup(["script", "style"]):
            script_or_style.decompose()
        # Get text, replacing block tags with newlines, stripping extra whitespace
        text = soup.get_text(separator='\n', strip=True)
        # Refine whitespace: reduce multiple newlines to max two
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        return text
    except Exception as e:
        logging.error(f"Error cleaning HTML with BeautifulSoup: {e}", exc_info=True)
        # Return raw HTML snippet on error? Or empty string? Empty is safer for AI.
        return "" # Return empty on error

# --- Main Scraping Function for a Single Job Page ---

def scrape_job_details(driver: WebDriver, job_url: str, config: dict, job_title_log: str, job_company_log: str) -> dict:
    """
    Navigates to a job URL and scrapes detailed information using config selectors.

    Args:
        driver: The Selenium WebDriver instance.
        job_url: The URL of the job posting to scrape.
        config: The master configuration dictionary.

    Returns:
        A dictionary containing scraped details. Includes an internal '_scrape_successful'
        key (True/False) and potentially an '_error_message' key on failure.
    """
    selectors = config['selectors']
    wait_time_long = config['selenium']['wait_time_long']
    wait_time_short = config['selenium']['wait_time_short']
    status_flags = config['status']

    logging.info(f"Navigating to job details page: {job_url}")
    details = {
        # Initialize all detail fields consistently
        'Applicant Count': 'N/A', 'Job Description HTML': '', 'Job Description Plain Text': '',
        'About Company': '', 'Date Scraped Detailed': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Posted Ago Text Detailed': 'N/A', 'Easy Apply': None, # Use None for undetermined boolean
        'Company LinkedIn URL': 'N/A', 'Company Industry': 'N/A', 'Company Size': 'N/A',
        'Company LinkedIn Members': 'N/A', 'Company Followers': 'N/A',
        'Hiring Team Member 1 Name': 'N/A', 'Hiring Team Member 1 Profile URL': 'N/A',
        'Hiring Team Member 2 Name': 'N/A', 'Hiring Team Member 2 Profile URL': 'N/A',
        'Skills Required': '', # Initialize as empty, AI might populate later
        '_scrape_successful': False, # Internal flag
        '_error_message': '' # Internal error tracking
    }


    try:
        driver.get(job_url)
        logging.info(f"Waiting up to {wait_time_long}s for job details container...")

        # Wait for the main container of the job details section
        details_container = WebDriverWait(driver, wait_time_long).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selectors['details_main_container']))
        )
        logging.debug("Main details container loaded.")
        time.sleep(get_random_delay(config, "short")) # Allow dynamic content

        # --- Scrolling (Optional but often helpful) ---
        # Perform a few scrolls to trigger lazy-loaded content like hiring team/company details
        logging.info("Scrolling down page to trigger potential lazy loading...")
        scroll_attempts = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        while scroll_attempts < 3: # Limit scrolls
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(get_random_delay(config, "medium")) # Wait for content load
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                logging.debug("Scroll height unchanged, likely reached bottom or no new content.")
                break
            last_height = new_height
            scroll_attempts += 1
        logging.debug("Finished scrolling attempts.")
        # Scroll back to top slightly might help ensure top elements are interactable if needed
        # driver.execute_script("window.scrollTo(0, 200);")
        # time.sleep(get_random_delay(config, "short"))

 
  # --- Extract Top Card Info (Applicants, Posted Ago, Company Link) ---
        logging.debug("Extracting top card info...")
        try:
            # First, ensure the top card container itself exists
            # Using a slightly more specific selector for the top card if possible
            top_card_selector = selectors.get('details_top_card', "div.jobs-details__main-content") # Default if not in config
            logging.debug(f" Attempting to find top card container with selector: {top_card_selector}")
            top_card = details_container.find_element(By.CSS_SELECTOR, top_card_selector)
            logging.debug(" Top card container element found.")

            # --- Attempt to Extract Company Link with Explicit Wait ---
            company_link_element = None # Initialize
            try:
                company_link_selector = selectors['details_company_link']
                logging.debug(f"  Waiting for Company Link element ('{company_link_selector}') within top card...")
                # Wait specifically for the link element to be present *within* the top_card
                company_link_element = WebDriverWait(top_card, wait_time_short).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, company_link_selector))
                )
                logging.debug(f"  Found potential company link element. OuterHTML (truncated): {safe_get_attribute(company_link_element, 'outerHTML')[:150]}...")
                # --- Add Small Delay - Just in Case ---
                time.sleep(0.5)
                # --------------------------------------
                details['Company LinkedIn URL'] = safe_get_attribute(company_link_element, 'href')
            except TimeoutException:
                logging.warning(f"  Timed out waiting for Company Link element ('{company_link_selector}') within top card.")
                details['Company LinkedIn URL'] = 'N/A (Timeout)' # Indicate specific failure reason
            except NoSuchElementException: # Should be caught by Wait, but as fallback
                logging.warning(f"  Company link element not found within top card using selector: {company_link_selector}")
                details['Company LinkedIn URL'] = 'N/A (Not Found)'
            except Exception as cle:
                 logging.error(f"  Unexpected error getting company link from top card: {cle}", exc_info=False)
                 details['Company LinkedIn URL'] = 'N/A (Error)'


            # --- Extract Metadata (Applicants/Posted Ago) ---
            logging.debug(" Extracting metadata...")
            try:
                metadata_container_selector = selectors['details_metadata_container']
                metadata_container = top_card.find_element(By.CSS_SELECTOR, metadata_container_selector)
                metadata_text = safe_get_text(metadata_container)
                logging.debug(f"  Metadata text: '{metadata_text}'")

                # Applicant Count (Keep previous regex logic)
                applicant_match = re.search(r'(?:(\d+)|Over\s+(\d+))\s+(applicant|people clicked apply)', metadata_text, re.IGNORECASE)
                if applicant_match:
                    num_only, over_num, _ = applicant_match.groups()
                    details['Applicant Count'] = f"+{over_num}" if over_num else num_only
                    logging.debug(f"  Found Applicant Count: {details['Applicant Count']}")
                else:
                    logging.debug("  Applicant count pattern not found.")

                # Posted Ago Text (Keep previous regex + fallback logic)
                time_match = re.search(r'(\d+\s+(?:hour|day|week|month|year)s?\s+ago|Just now|Yesterday)', metadata_text, re.IGNORECASE)
                if time_match:
                    details['Posted Ago Text Detailed'] = time_match.group(0).strip()
                    logging.debug(f"  Found Posted Ago (Metadata): {details['Posted Ago Text Detailed']}")
                else:
                     try:
                         time_el_selector = selectors['details_posted_ago_fallback']
                         time_el = top_card.find_element(By.CSS_SELECTOR, time_el_selector)
                         posted_ago_text = safe_get_text(time_el)
                         if posted_ago_text:
                            details['Posted Ago Text Detailed'] = posted_ago_text
                            logging.debug(f"  Found Posted Ago (Fallback Span): {details['Posted Ago Text Detailed']}")
                         else:
                             logging.debug("  Posted Ago fallback span found but empty.")
                     except NoSuchElementException:
                         logging.debug("  Posted Ago fallback span not found.")

            except NoSuchElementException:
                 logging.warning("  Metadata container ('{}') not found within top card.".format(metadata_container_selector))
            except Exception as meta_err:
                logging.error(f"  Error parsing metadata: {meta_err}", exc_info=False)
            # --- End Metadata Extraction ---

        except NoSuchElementException:
            logging.warning("Top card container ('{}') not found. Cannot extract details.".format(top_card_selector))
            # Set fields to error state if top card fails? Optional.
            # details['Company LinkedIn URL'] = 'N/A (Top Card Fail)'
        except Exception as e:
            logging.error(f"Unexpected error parsing top card info: {e}", exc_info=True)

        # --- Easy Apply Button Check ---
        logging.debug("Checking for Easy Apply button...")
        try:
            # Wait briefly for the button to potentially appear/become interactable
            easy_apply_button = WebDriverWait(driver, wait_time_short).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selectors['details_easy_apply_button']))
            )
            details['Easy Apply'] = easy_apply_button.is_displayed()
            logging.debug(f"  Easy Apply button found (Displayed: {details['Easy Apply']}).")
        except TimeoutException:
            details['Easy Apply'] = False
            logging.debug("  Easy Apply button not found (Timeout).")
        except Exception as e:
            logging.warning(f"  Error checking Easy Apply button: {e}", exc_info=False)
            details['Easy Apply'] = None # Indicate error


        # --- Job Description ---
        logging.debug("Extracting job description...")
        try:
            # Click "Show more" for description if present
            try:
                show_more_button = WebDriverWait(driver, wait_time_short).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selectors['details_show_more_button']))
                )
                # Scroll into view before clicking
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", show_more_button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", show_more_button) # JS click preferred
                logging.debug("  Clicked 'Show more' for description.")
                time.sleep(get_random_delay(config, "medium")) # Wait for expansion
            except TimeoutException:
                logging.debug("  'Show more' description button not found or not clickable.")
            except ElementClickInterceptedException:
                 logging.warning("  'Show more' description button found but could not be clicked (intercepted). Content might be partial.")
            except Exception as e:
                 logging.error(f"  Error clicking 'Show more' for description: {e}", exc_info=True)

            # Get description element HTML and process to Plain Text
            description_element = details_container.find_element(By.CSS_SELECTOR, selectors['details_description_container'])
            html_content = safe_get_attribute(description_element, 'innerHTML')
            details['Job Description HTML'] = html_content
            details['Job Description Plain Text'] = clean_html_for_text(html_content)
            logging.debug(f"  Processed Job Description (HTML: {len(details['Job Description HTML'])} chars, Text: {len(details['Job Description Plain Text'])} chars).")

        except NoSuchElementException:
            logging.error(f"Job Description container ('{selectors['details_description_container']}') not found.")
            details['_error_message'] = status_flags.get("MISSING_DATA", "Error - Missing JD Text")
        except Exception as e:
            logging.error(f"Error getting/parsing Job Description: {e}", exc_info=True)
            details['_error_message'] = status_flags.get("FAILED_SCRAPE_DETAILS", "Error - Scrape Details")


        # --- Company Details Section ---
        logging.debug("Extracting company details section...")
        try:
            company_section = details_container.find_element(By.CSS_SELECTOR, selectors['details_company_section'])
            logging.debug("  Found company section container.")

            # Followers (from subtitle)
            try:
                 followers_subtitle = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_followers_subtitle'])
                 followers_text = safe_get_text(followers_subtitle)
                 followers_match = re.search(r'([\d,]+)\s+followers', followers_text)
                 if followers_match:
                      details['Company Followers'] = followers_match.group(1).replace(',', '')
                      logging.debug(f"    Found Followers: {details['Company Followers']}")
            except NoSuchElementException: logging.debug("    Followers subtitle not found.")
            except Exception as e: logging.warning(f"    Error getting followers: {e}")

            # Industry, Size, Members (from info div)
            try:
                 info_div = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_info_div'])
                 info_text = safe_get_text(info_div).replace('\n', ' ') # Clean text

                 # Industry
                 industry_match = re.match(r'^(.*?)(?:\s+\d{1,3}(?:,\d{3})*\s*-\s*\d{1,3}(?:,\d{3})*\s+employees|\s+\d{1,3}(?:,\d{3})*\s+on LinkedIn)', info_text)
                 if industry_match:
                     details['Company Industry'] = industry_match.group(1).strip(' ·')
                     logging.debug(f"    Found Industry: {details['Company Industry']}")

                 # Size
                 size_match = re.search(r'(\d{1,3}(?:,\d{3})*(?:\s*-\s*\d{1,3}(?:,\d{3})*)?)\s+employees', info_text, re.IGNORECASE)
                 if size_match:
                     details['Company Size'] = size_match.group(1).strip()
                     logging.debug(f"    Found Size: {details['Company Size']}")

                 # LinkedIn Members
                 members_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s+on LinkedIn', info_text, re.IGNORECASE)
                 if members_match:
                     details['Company LinkedIn Members'] = members_match.group(1).replace(',', '')
                     logging.debug(f"    Found LinkedIn Members: {details['Company LinkedIn Members']}")

            except NoSuchElementException: logging.debug("    Company info div not found.")
            except Exception as e: logging.warning(f"    Error parsing company info div: {e}")

             # About Company Text
            try:
                 about_text_element = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_about_text'])
                 # Click "Show more" for company description if needed
                 try:
                      show_more_about_button = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_show_more_button'])
                      if show_more_about_button.is_displayed():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", show_more_about_button); time.sleep(0.5)
                            driver.execute_script("arguments[0].click();", show_more_about_button)
                            logging.debug("      Clicked 'Show more' for About Company.")
                            time.sleep(get_random_delay(config, "short"))
                            # Re-fetch element after potential DOM change
                            about_text_element = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_about_text'])
                 except NoSuchElementException: pass # No show more button
                 except Exception as sm_err: logging.warning(f"      Error clicking 'Show more' for About Company: {sm_err}")

                 about_html = safe_get_attribute(about_text_element, 'innerHTML')
                 details['About Company'] = clean_html_for_text(about_html)
                 logging.debug(f"    Found About Company Text (length: {len(details['About Company'])}).")
            except NoSuchElementException: logging.debug("    About Company description paragraph not found.")
            except Exception as e: logging.warning(f"    Error getting About Company text: {e}")

        except NoSuchElementException:
             logging.debug("Company details section container not found.")
        except Exception as e:
             logging.warning(f"Error processing company details section: {e}", exc_info=True)


        # --- Meet the Hiring Team Section ---
        logging.debug("Extracting hiring team info...")
        try:
            hiring_team_section = details_container.find_element(By.XPATH, selectors['details_hiring_team_section_xpath'])
            logging.debug("  Found hiring team section container.")

            member_cards = hiring_team_section.find_elements(By.CSS_SELECTOR, selectors['details_hiring_team_card'])
            logging.debug(f"  Found {len(member_cards)} potential hiring team member card(s).")

            for i, card in enumerate(member_cards):
                if i >= 2: break # Limit to first two members
                member_index = i + 1
                try:
                    name_element = card.find_element(By.CSS_SELECTOR, selectors['details_hiring_team_name'])
                    # Link is often around the whole card or name/image
                    profile_link_element = card.find_element(By.CSS_SELECTOR, selectors['details_hiring_team_profile_link'])

                    name = safe_get_text(name_element)
                    profile_url = safe_get_attribute(profile_link_element, 'href')
                    # Clean URL
                    if profile_url and '?' in profile_url: profile_url = profile_url.split('?')[0]

                    details[f'Hiring Team Member {member_index} Name'] = name
                    details[f'Hiring Team Member {member_index} Profile URL'] = profile_url
                    logging.debug(f"    Found Member {member_index}: Name='{name}', URL='{profile_url}'")
                except NoSuchElementException:
                    logging.debug(f"    Could not extract name/URL elements for member {member_index}.")
                except Exception as member_err:
                     logging.warning(f"    Error processing member {member_index}: {member_err}")

        except NoSuchElementException:
            logging.debug("'Meet the hiring team' section not found.")
        except Exception as e:
            logging.warning(f"Error processing hiring team section: {e}", exc_info=True)


        # Mark scrape as successful if we got this far without critical errors
        details['_scrape_successful'] = True
        logging.info(f"Successfully scraped details for:  '{job_title_log}' | Company:{job_company_log} ---")


    except TimeoutException:
        logging.error(f"Timeout waiting for core page elements on {job_url}.")
        details['_scrape_successful'] = False
        details['_error_message'] = status_flags.get("FAILED_SCRAPE_DETAILS", "Error - Scrape Timeout")
    except WebDriverException as e:
         logging.critical(f"WebDriverException occurred while scraping {job_url}: {e}")
         details['_scrape_successful'] = False
         details['_error_message'] = status_flags.get("FAILED_WEBDRIVER", "Error - WebDriver Exception")
         raise e # Re-raise critical WebDriver exceptions to potentially stop the phase
    except Exception as e:
        logging.error(f"Unexpected error scraping details for {job_url}: {e}", exc_info=True)
        details['_scrape_successful'] = False
        details['_error_message'] = status_flags.get("FAILED_SCRAPE_DETAILS", "Error - Scrape Details")

    return details

# --- Main Processing Function for the Excel File ---

# **** START REPLACEMENT for process_excel_for_details function in phase2_detail_scraper.py ****
def process_excel_for_details(driver: WebDriver, config: dict):
    """Reads Excel, scrapes details for each job link, updates the DataFrame and saves."""
    # --- Config Extraction ---
    excel_filepath = config['paths']['excel_filepath']
    status_new = config['status']['NEW']
    status_processing = config['status']['PROCESSING_DETAILS']
    status_ready_for_ai = config['status']['READY_FOR_AI']
    status_invalid_link = config['status']['INVALID_LINK']
    status_scrape_failed = config['status']['FAILED_SCRAPE_DETAILS'] # Base scrape failure status
    # Add other potential phase 2 error statuses if defined
    phase2_error_statuses = [
        status_scrape_failed,
        config['status'].get('FAILED_WEBDRIVER'),
        config['status'].get('MISSING_DATA'), # e.g., If JD text is crucial later but missing
        status_invalid_link # Also retry invalid links maybe? Optional.
        ]
    phase2_error_statuses = [s for s in phase2_error_statuses if s] # Filter out None if status doesn't exist

    save_interval = config['phase2']['save_interval']
    retry_failed = config['workflow']['retry_failed_phase2'] # Get retry flag

    logging.info(f"Starting detailed scraping process for Excel file: {excel_filepath}")
    logging.info(f"Retry previously failed rows: {retry_failed}")

    try:
        if not os.path.exists(excel_filepath):
            logging.error(f"Input Excel file not found: '{excel_filepath}'. Phase 2 cannot run.")
            return False # Critical failure

        logging.info("Reading Excel file...")
        # Specify dtype=object to prevent pandas from guessing types incorrectly
        df = pd.read_excel(excel_filepath, engine='openpyxl', dtype=object)
        logging.info(f"Read {len(df)} rows from Excel.")

        # --- Schema Check & Update ---
        added_cols = False
        current_columns = list(df.columns) # Store original order for comparison
        for col in ALL_EXPECTED_COLUMNS:
             if col not in df.columns:
                  logging.warning(f"Adding missing column '{col}' to DataFrame.")
                  # Initialize similarly to phase 1
                  if col in ['Notes', 'Job Description HTML', 'Job Description Plain Text', 'About Company', 'Skills Required', 'Applied Date', 'Extracted Responsibilities', 'Extracted Required Skills', 'Extracted Preferred Skills', 'Extracted Experience Level', 'Extracted Key Qualifications', 'Extracted Company Description', 'AI Score Justification', 'AI Strengths', 'AI Areas for Improvement', 'AI Actionable Recommendations', 'Generated Tailored Summary', 'Generated Tailored Bullets', 'Generated Tailored Skills List', 'Tailored HTML Path', 'Tailored PDF Path']:
                        df[col] = ''
                  else:
                        df[col] = pd.NA
                  added_cols = True
        if added_cols or list(df.columns) != ALL_EXPECTED_COLUMNS:
             logging.info("Reordering DataFrame columns to standard order.")
             df = df.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='') # Use fill_value=''

        # --- Fill NA/NaN in Crucial Columns ---
        df['Status'] = df['Status'].fillna(status_new) # Assume blank status is 'New'
        df['Link'] = df['Link'].fillna('')
        df['AI Match Score'] = pd.to_numeric(df['AI Match Score'], errors='coerce').fillna(-1.0) # Example fill

        # --- Filter rows to process ---
        statuses_to_process = [status_new]
        if retry_failed:
            statuses_to_process.extend(phase2_error_statuses)
            # Remove duplicates just in case status_new is also an error status somehow
            statuses_to_process = list(set(statuses_to_process))
            logging.info(f"Will process jobs with status in: {statuses_to_process}")
        else:
            logging.info(f"Will process jobs with status: ['{status_new}']")

        rows_to_process_mask = df['Status'].isin(statuses_to_process)
        rows_to_process_idx = df[rows_to_process_mask].index
        num_to_process = len(rows_to_process_idx)
        logging.info(f"Found {num_to_process} rows matching processing criteria.")


        if num_to_process == 0:
            logging.info("No jobs found to process in this phase.")
            if added_cols: # Save if schema changed
                try:
                    logging.info("Saving Excel file due to schema changes even though no rows were processed.")
                    df.to_excel(excel_filepath, index=False, engine='openpyxl')
                    return True
                except PermissionError: logging.error(f"PERM ERROR saving schema changes to {excel_filepath}."); return False
                except Exception as save_err: logging.error(f"Error saving schema changes: {save_err}", exc_info=True); return False
            else:
                return True # Nothing to do, phase successful

        # --- Processing Loop ---
        update_count = 0
        processed_in_run = 0 # Counter for total processed in this run
        batch_start_time = time.time()

        for index in rows_to_process_idx:
            processed_in_run += 1
            job_link = df.loc[index, 'Link']
            job_Company = df.loc[index, 'Company'] # For logging
            job_title = df.loc[index, 'Title'] # For logging


            logging.info(f"--- Processing Row {index + 1}/{len(df)} (Index: {index}) | Job: '{job_title}' | Company:{job_Company} ---")

            if pd.isna(job_link) or not isinstance(job_link, str) or not job_link.strip().startswith('http'):
                logging.warning(f"Skipping row {index + 1} due to invalid/missing link: '{job_link}'")
                df.loc[index, 'Status'] = status_invalid_link
                continue # Skip to next row

            df.loc[index, 'Status'] = status_processing # Set status BEFORE scraping

            try:
                scraped_details = scrape_job_details(driver, job_link, config, job_title_log=job_title, job_company_log=job_Company)

                if scraped_details['_scrape_successful']:
                    for col, value in scraped_details.items():
                        if col in df.columns and not col.startswith('_'):
                            df.loc[index, col] = value
                    df.loc[index, 'Status'] = status_ready_for_ai
                    update_count += 1
                    logging.info(f"Successfully processed row {index+1}. Status: '{status_ready_for_ai}'.")
                else:
                     error_msg = scraped_details.get('_error_message', status_scrape_failed)
                     df.loc[index, 'Status'] = error_msg # Use specific error if available
                     logging.error(f"Scraping failed for row {index+1}. Status: '{df.loc[index, 'Status']}'. Link: {job_link}")
                     if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"Phase 2 Error: {error_msg}"

                # --- Periodic Save ---
                # Save based on total processed in this run, not just successful ones in batch
                if processed_in_run % save_interval == 0:
                    batch_time = time.time() - batch_start_time
                    logging.info(f"Processed {processed_in_run} rows so far ({batch_time:.2f} sec). Saving progress...")
                    try:
                        df.to_excel(excel_filepath, index=False, engine='openpyxl')
                        logging.info("Progress saved successfully.")
                        batch_start_time = time.time()
                    except PermissionError: logging.error(f"PERM ERROR saving progress to {excel_filepath}. Stopping phase."); return False
                    except Exception as save_err: logging.error(f"Error saving progress: {save_err}", exc_info=True); logging.warning("Continuing processing...")

                delay = get_random_delay(config, "long")
                logging.debug(f"Waiting {delay:.2f} seconds before next job...")
                time.sleep(delay)

            except WebDriverException as wd_exc:
                 logging.critical(f"WebDriverException during processing row {index+1}. Stopping phase.")
                 logging.critical(traceback.format_exc())
                 df.loc[index, 'Status'] = config['status'].get("FAILED_WEBDRIVER", "Error - WebDriver Exception")
                 try:
                     logging.info("Attempting final save after WebDriverException...")
                     df.to_excel(excel_filepath, index=False, engine='openpyxl')
                     logging.info("Final save attempted.")
                 except Exception as final_save_err: logging.error(f"Error during final save attempt: {final_save_err}")
                 return False # Critical failure
            except Exception as row_err:
                 logging.error(f"Unexpected error processing row {index+1} for link {job_link}: {row_err}", exc_info=True)
                 df.loc[index, 'Status'] = config['status'].get("UNKNOWN_ERROR", "Error - Unknown")
                 if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"Phase 2 Error: {str(row_err)[:250]}"
                 continue # Continue to the next row

        # --- Final Save After Loop ---
        logging.info("Finished processing all designated rows. Performing final save...")
        try:
             df.to_excel(excel_filepath, index=False, engine='openpyxl')
             logging.info("Final Excel file saved successfully.")
        except PermissionError: logging.error(f"FINAL SAVE ERROR: Permission denied: {excel_filepath}."); return False
        except Exception as save_err: logging.error(f"Error during final save: {save_err}", exc_info=True); return False

        logging.info(f"Phase 2 finished. Successfully updated details for {update_count} out of {num_to_process} targeted rows.")
        return True

    except FileNotFoundError: logging.error(f"Excel file not found at start: '{excel_filepath}'."); return False
    except KeyError as e: logging.error(f"Missing key in config/DataFrame: {e}", exc_info=True); return False
    except Exception as e: logging.critical(f"Crit error during Phase 2 setup: {e}", exc_info=True); return False
# **** END REPLACEMENT for process_excel_for_details function in phase2_detail_scraper.py ****

# --- Main Function for Phase 2 ---
def run_phase2_detail_scraping(config: dict) -> bool:
    """
    Executes the Phase 2 workflow: connect to Selenium, read Excel,
    scrape details for 'New' jobs, update status, and save.

    Args:
        config: The master configuration dictionary.

    Returns:
        True if the phase completed its processing run (even with row-level errors),
        False if a critical error prevented processing (e.g., file not found,
        WebDriver crash, critical save error).
    """
    logging.info("Initiating Phase 2: Job Detail Scraping")
    driver = None # Driver needs to be managed (likely connected in Phase 1)
    overall_success = False

    # --- Get Driver Reference (Assume it's passed or globally available - Needs Refinement) ---
    # How the driver is passed between phases needs a clear strategy.
    # Option 1: Pass driver object via config (can be complex)
    # Option 2: Re-connect in each phase (simple but requires browser to stay open)
    # Option 3: Global variable (generally discouraged)
    # Assuming Option 2 for now: Re-connect using debugger port
    # (Note: Phase 1's setup_selenium_driver is identical, could be moved to a common utils file)
    from phase1_list_scraper import setup_selenium_driver # Reuse connector function
    driver = setup_selenium_driver(config)

    if not driver:
        logging.critical("Failed to connect to Selenium WebDriver for Phase 2. Cannot proceed.")
        return False

    # --- Start Processing ---
    try:
        overall_success = process_excel_for_details(driver, config)
    except WebDriverException as e:
        # Catch WebDriver errors propagated from process_excel_for_details
        logging.critical(f"WebDriverException caught in run_phase2: {e}")
        overall_success = False
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in run_phase2: {e}")
        logging.critical(traceback.format_exc())
        overall_success = False
    # Do NOT driver.quit() here if using debugger port

    if overall_success:
        logging.info("Phase 2 processing run completed.")
    else:
        logging.error("Phase 2 processing run finished with critical errors.")

    return overall_success

# No `if __name__ == "__main__":` block needed