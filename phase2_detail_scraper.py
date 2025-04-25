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

# Import the potentially enhanced driver setup function
# Ensure phase1_list_scraper.py is updated first
from phase1_list_scraper import setup_selenium_driver, get_random_delay

# MODIFIED (Proposals #7, #8, #10, #11): Define ALL columns expected - MUST MATCH phase1_list_scraper.py
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
# get_random_delay is now imported from phase1_list_scraper

def safe_get_text(element: WebElement) -> str:
    """Safely get text from a Selenium element, returning empty string on error."""
    # (No changes needed)
    try:
        return element.text.strip()
    except (NoSuchElementException, StaleElementReferenceException):
        return ""
    except Exception as e:
        logging.warning(f"Unexpected error getting text from element: {e}", exc_info=False)
        return ""

def safe_get_attribute(element: WebElement, attribute: str) -> str:
    """Safely get an attribute from a Selenium element, returning empty string on error."""
    # (No changes needed)
    try:
        return element.get_attribute(attribute) or ""
    except (NoSuchElementException, StaleElementReferenceException):
        return ""
    except Exception as e:
        logging.warning(f"Unexpected error getting attribute '{attribute}' from element: {e}", exc_info=False)
        return ""

def clean_html_for_text(html_content: str) -> str:
    """Uses BeautifulSoup to extract clean plain text from HTML content."""
    # (No changes needed)
    if not html_content or pd.isna(html_content): return ""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        for script_or_style in soup(["script", "style"]): script_or_style.decompose()
        text = soup.get_text(separator='\n', strip=True)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        return text
    except Exception as e:
        logging.error(f"Error cleaning HTML with BeautifulSoup: {e}", exc_info=True)
        return ""

# --- Main Scraping Function for a Single Job Page ---

# MODIFIED (Proposals #7, #11): Refined error logging, consolidated hiring team
def scrape_job_details(driver: WebDriver, job_url: str, config: dict, job_title_log: str, job_company_log: str) -> dict:
    """
    Navigates to a job URL and scrapes detailed information using config selectors.
    Logs warnings for optional field failures and records issues.
    """
    selectors = config['selectors']
    # Use reduced wait time from config
    wait_time_long = config['selenium']['wait_time_long'] # Default 20s now
    wait_time_short = config['selenium']['wait_time_short']
    status_flags = config['status']

    logging.info(f"Navigating to job details page: {job_url}")
    # Initialize details dictionary - removed member 2, added Scraping Issues
    details = {
        'Applicant Count': 'N/A', 'Job Description HTML': '', 'Job Description Plain Text': '',
        'About Company': '', 'Date Scraped Detailed': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Posted Ago Text Detailed': 'N/A', 'Easy Apply': None,
        'Company LinkedIn URL': 'N/A', 'Company Industry': 'N/A', 'Company Size': 'N/A',
        'Company LinkedIn Members': 'N/A', 'Company Followers': 'N/A',
        'Hiring Team Member 1 Name': 'N/A', 'Hiring Team Member 1 Profile URL': 'N/A',
        'Scraping Issues': '', # New field for recording issues
        '_scrape_successful': False, # Internal flag
        '_error_message': '' # Internal error tracking for critical failures
    }
    scraping_issues_list = [] # Track non-critical issues


    try:
        driver.get(job_url)
        logging.info(f"Waiting up to {wait_time_long}s for job details container...")

        # Wait for the main container - Critical Element
        details_container = WebDriverWait(driver, wait_time_long).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selectors['details_main_container']))
        )
        logging.debug("Main details container loaded.")
        time.sleep(get_random_delay(config, "short")) # Allow dynamic content

        # Scrolling (remains the same)
        logging.info("Scrolling down page to trigger potential lazy loading...")
        scroll_attempts = 0; last_height = driver.execute_script("return document.body.scrollHeight")
        while scroll_attempts < 3:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(get_random_delay(config, "medium"))
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: break
            last_height = new_height; scroll_attempts += 1
        logging.debug("Finished scrolling attempts.")


        # --- Extract Top Card Info --- Optional Fields Handled with Warnings
        logging.debug("Extracting top card info...")
        try:
            top_card_selector = selectors.get('details_top_card', "div.jobs-details__main-content")
            top_card = details_container.find_element(By.CSS_SELECTOR, top_card_selector)

            # --- Company Link (Optional) ---
            try:
                company_link_selector = selectors['details_company_link']
                company_link_element = WebDriverWait(top_card, wait_time_short).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, company_link_selector))
                )
                time.sleep(0.5)
                details['Company LinkedIn URL'] = safe_get_attribute(company_link_element, 'href')
                logging.debug(f"  Found Company Link: {details['Company LinkedIn URL']}")
            except (TimeoutException, NoSuchElementException):
                logging.warning(f"  Could not find Company Link ('{company_link_selector}').")
                scraping_issues_list.append("Company Link")
                details['Company LinkedIn URL'] = 'N/A'
            except Exception as cle:
                 logging.warning(f"  Unexpected error getting company link: {cle}", exc_info=False)
                 scraping_issues_list.append("Company Link (Error)")
                 details['Company LinkedIn URL'] = 'N/A'

            # --- Metadata (Applicants/Posted Ago) (Optional) ---
            try:
                metadata_container_selector = selectors['details_metadata_container']
                metadata_container = top_card.find_element(By.CSS_SELECTOR, metadata_container_selector)
                metadata_text = safe_get_text(metadata_container)
                logging.debug(f"  Metadata text: '{metadata_text}'")

                # Applicant Count
                applicant_match = re.search(r'(?:(\d+)|Over\s+(\d+))\s+(applicant|people clicked apply)', metadata_text, re.IGNORECASE)
                if applicant_match:
                    num_only, over_num, _ = applicant_match.groups()
                    details['Applicant Count'] = f"+{over_num}" if over_num else num_only
                    logging.debug(f"  Found Applicant Count: {details['Applicant Count']}")
                else: logging.debug("  Applicant count pattern not found.")

                # Posted Ago Text
                time_match = re.search(r'(\d+\s+(?:hour|day|week|month|year)s?\s+ago|Just now|Yesterday)', metadata_text, re.IGNORECASE)
                if time_match:
                    details['Posted Ago Text Detailed'] = time_match.group(0).strip()
                    logging.debug(f"  Found Posted Ago (Metadata): {details['Posted Ago Text Detailed']}")
                else: # Fallback
                     try:
                         time_el_selector = selectors['details_posted_ago_fallback']
                         time_el = top_card.find_element(By.CSS_SELECTOR, time_el_selector)
                         posted_ago_text = safe_get_text(time_el)
                         if posted_ago_text: details['Posted Ago Text Detailed'] = posted_ago_text; logging.debug("  Found Posted Ago (Fallback Span).")
                         else: logging.debug("  Posted Ago fallback span found but empty.")
                     except NoSuchElementException: logging.debug("  Posted Ago fallback span not found.")

            except NoSuchElementException:
                 logging.warning(f"  Metadata container ('{metadata_container_selector}') not found.")
                 scraping_issues_list.append("Metadata Container")
            except Exception as meta_err:
                logging.warning(f"  Error parsing metadata: {meta_err}", exc_info=False)
                scraping_issues_list.append("Metadata Parsing Error")

        except NoSuchElementException:
            logging.warning(f"Top card container ('{top_card_selector}') not found. Some details may be missing.")
            scraping_issues_list.append("Top Card Container")
        except Exception as e:
            logging.error(f"Unexpected error parsing top card info: {e}", exc_info=True) # Log as error if structure breaks
            scraping_issues_list.append("Top Card Parsing Error")


        # --- Easy Apply Button Check (Optional) ---
        logging.debug("Checking for Easy Apply button...")
        try:
            easy_apply_button = WebDriverWait(driver, wait_time_short).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selectors['details_easy_apply_button']))
            )
            details['Easy Apply'] = easy_apply_button.is_displayed()
            logging.debug(f"  Easy Apply button found (Displayed: {details['Easy Apply']}).")
        except TimeoutException: details['Easy Apply'] = False; logging.debug("  Easy Apply button not found (Timeout).")
        except Exception as e: logging.warning(f"  Error checking Easy Apply button: {e}", exc_info=False); details['Easy Apply'] = None; scraping_issues_list.append("Easy Apply Check Error")


        # --- Job Description (Critical) ---
        logging.debug("Extracting job description...")
        try:
            # Click "Show more" (Optional action within critical block)
            try:
                show_more_button = WebDriverWait(driver, wait_time_short).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selectors['details_show_more_button']))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", show_more_button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", show_more_button)
                logging.debug("  Clicked 'Show more' for description.")
                time.sleep(get_random_delay(config, "medium"))
            except TimeoutException: logging.debug("  'Show more' description button not found or not clickable.")
            except ElementClickInterceptedException: logging.warning("  'Show more' description button intercepted. Content might be partial."); scraping_issues_list.append("Desc Show More Click Intercepted")
            except Exception as e: logging.error(f"  Error clicking 'Show more' for description: {e}", exc_info=True); scraping_issues_list.append("Desc Show More Click Error") # Log as error

            # Get description element HTML (Critical action)
            description_element = details_container.find_element(By.CSS_SELECTOR, selectors['details_description_container'])
            html_content = safe_get_attribute(description_element, 'innerHTML')
            details['Job Description HTML'] = html_content
            plain_text = clean_html_for_text(html_content)
            if not plain_text or len(plain_text) < 20: # Add check for meaningful text length
                 logging.warning("Extracted job description plain text is very short or empty.")
                 details['Job Description Plain Text'] = plain_text or " " # Assign empty or space if really empty
                 scraping_issues_list.append("JD Text Short/Empty")
            else:
                 details['Job Description Plain Text'] = plain_text
                 logging.debug(f"  Processed Job Description (HTML: {len(details['Job Description HTML'])} chars, Text: {len(details['Job Description Plain Text'])} chars).")

        except NoSuchElementException:
            logging.error(f"CRITICAL: Job Description container ('{selectors['details_description_container']}') not found.")
            details['_error_message'] = status_flags.get("MISSING_DATA", "Error - Missing JD Text")
            details['_scrape_successful'] = False # Mark as critical failure
            return details # Exit early if JD missing
        except Exception as e:
            logging.error(f"CRITICAL: Error getting/parsing Job Description: {e}", exc_info=True)
            details['_error_message'] = status_flags.get("FAILED_SCRAPE_DETAILS", "Error - Scrape Details")
            details['_scrape_successful'] = False
            return details # Exit early on critical JD error


        # --- Company Details Section (Optional) ---
        logging.debug("Extracting company details section...")
        try:
            company_section = details_container.find_element(By.CSS_SELECTOR, selectors['details_company_section'])
            logging.debug("  Found company section container.")

            # Followers (Optional)
            try:
                 followers_subtitle = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_followers_subtitle'])
                 followers_text = safe_get_text(followers_subtitle)
                 followers_match = re.search(r'([\d,]+)\s+followers', followers_text)
                 if followers_match: details['Company Followers'] = followers_match.group(1).replace(',', ''); logging.debug(f"    Found Followers: {details['Company Followers']}")
            except NoSuchElementException: logging.debug("    Followers subtitle not found.")
            except Exception as e: logging.warning(f"    Error getting followers: {e}"); scraping_issues_list.append("Company Followers")

            # Industry, Size, Members (Optional)
            try:
                 info_div = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_info_div'])
                 info_text = safe_get_text(info_div).replace('\n', ' ')
                 industry_match = re.match(r'^(.*?)(?:\s+\d{1,3}(?:,\d{3})*\s*-\s*\d{1,3}(?:,\d{3})*\s+employees|\s+\d{1,3}(?:,\d{3})*\s+on LinkedIn)', info_text)
                 if industry_match: details['Company Industry'] = industry_match.group(1).strip(' ·'); logging.debug(f"    Found Industry: {details['Company Industry']}")
                 size_match = re.search(r'(\d{1,3}(?:,\d{3})*(?:\s*-\s*\d{1,3}(?:,\d{3})*)?)\s+employees', info_text, re.IGNORECASE)
                 if size_match: details['Company Size'] = size_match.group(1).strip(); logging.debug(f"    Found Size: {details['Company Size']}")
                 members_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s+on LinkedIn', info_text, re.IGNORECASE)
                 if members_match: details['Company LinkedIn Members'] = members_match.group(1).replace(',', ''); logging.debug(f"    Found LinkedIn Members: {details['Company LinkedIn Members']}")
            except NoSuchElementException: logging.debug("    Company info div not found.")
            except Exception as e: logging.warning(f"    Error parsing company info div: {e}"); scraping_issues_list.append("Company Info Div")

             # About Company Text (Optional)
            try:
                 about_text_element = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_about_text'])
                 try: # Click "Show more" for about (Optional action)
                      show_more_about_button = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_show_more_button'])
                      if show_more_about_button.is_displayed():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", show_more_about_button); time.sleep(0.5)
                            driver.execute_script("arguments[0].click();", show_more_about_button)
                            logging.debug("      Clicked 'Show more' for About Company.")
                            time.sleep(get_random_delay(config, "short"))
                            about_text_element = company_section.find_element(By.CSS_SELECTOR, selectors['details_company_about_text']) # Re-fetch
                 except NoSuchElementException: pass
                 except Exception as sm_err: logging.warning(f"      Error clicking 'Show more' for About Company: {sm_err}"); scraping_issues_list.append("About Show More Click Error")
                 about_html = safe_get_attribute(about_text_element, 'innerHTML')
                 details['About Company'] = clean_html_for_text(about_html)
                 logging.debug(f"    Found About Company Text (length: {len(details['About Company'])}).")
            except NoSuchElementException: logging.debug("    About Company description paragraph not found.")
            except Exception as e: logging.warning(f"    Error getting About Company text: {e}"); scraping_issues_list.append("About Company Text")

        except NoSuchElementException:
             logging.debug("Company details section container not found.")
             scraping_issues_list.append("Company Section")
        except Exception as e:
             logging.warning(f"Error processing company details section: {e}", exc_info=False)
             scraping_issues_list.append("Company Section Error")


        # --- Meet the Hiring Team Section (Optional) ---
        # MODIFIED (Proposal #11): Consolidate member info
        logging.debug("Extracting hiring team info...")
        try:
            hiring_team_section = details_container.find_element(By.XPATH, selectors['details_hiring_team_section_xpath'])
            logging.debug("  Found hiring team section container.")
            member_cards = hiring_team_section.find_elements(By.CSS_SELECTOR, selectors['details_hiring_team_card'])
            logging.debug(f"  Found {len(member_cards)} potential hiring team member card(s).")

            members_data = []
            for i, card in enumerate(member_cards):
                if i >= 2: break # Limit to first two members for consolidation
                try:
                    name_element = card.find_element(By.CSS_SELECTOR, selectors['details_hiring_team_name'])
                    profile_link_element = card.find_element(By.CSS_SELECTOR, selectors['details_hiring_team_profile_link'])
                    name = safe_get_text(name_element)
                    profile_url = safe_get_attribute(profile_link_element, 'href')
                    if profile_url and '?' in profile_url: profile_url = profile_url.split('?')[0] # Clean URL
                    if name and profile_url:
                        members_data.append({'name': name, 'url': profile_url})
                        logging.debug(f"    Found Member {i+1}: Name='{name}', URL='{profile_url}'")
                    else:
                        logging.debug(f"    Could not extract full details for member {i+1}.")
                except NoSuchElementException: logging.debug(f"    Could not extract name/URL elements for member {i+1}.")
                except Exception as member_err: logging.warning(f"    Error processing member {i+1}: {member_err}"); scraping_issues_list.append(f"Hiring Team Member {i+1}")

            # Consolidate into first member fields
            if len(members_data) >= 1:
                details['Hiring Team Member 1 Name'] = members_data[0]['name']
                details['Hiring Team Member 1 Profile URL'] = members_data[0]['url']
            if len(members_data) == 2:
                # Append second member's info with a delimiter
                details['Hiring Team Member 1 Name'] += f"; {members_data[1]['name']}"
                details['Hiring Team Member 1 Profile URL'] += f"; {members_data[1]['url']}"
                logging.debug("    Appended second hiring team member info to first member fields.")

        except NoSuchElementException:
            logging.debug("'Meet the hiring team' section not found.")
            # Don't add to issues if section simply doesn't exist
        except Exception as e:
            logging.warning(f"Error processing hiring team section: {e}", exc_info=False)
            scraping_issues_list.append("Hiring Team Section Error")


        # Mark scrape as successful if we got this far (JD was extracted)
        details['_scrape_successful'] = True
        details['Scraping Issues'] = ", ".join(scraping_issues_list) if scraping_issues_list else "" # Join issues
        logging.info(f"Successfully processed details for: '{job_title_log}' | Company: '{job_company_log}'")
        if details['Scraping Issues']:
            logging.warning(f"  Scraping issues encountered: {details['Scraping Issues']}")


    except TimeoutException:
        logging.error(f"Timeout waiting for critical page elements on {job_url}.")
        details['_scrape_successful'] = False
        details['_error_message'] = status_flags.get("FAILED_SCRAPE_DETAILS", "Error - Scrape Timeout")
    except WebDriverException as e:
         logging.critical(f"WebDriverException occurred while scraping {job_url}: {e}")
         details['_scrape_successful'] = False
         details['_error_message'] = status_flags.get("FAILED_WEBDRIVER", "Error - WebDriver Exception")
         raise e # Re-raise critical WebDriver exceptions
    except Exception as e:
        logging.error(f"Unexpected critical error scraping details for {job_url}: {e}", exc_info=True)
        details['_scrape_successful'] = False
        details['_error_message'] = status_flags.get("FAILED_SCRAPE_DETAILS", "Error - Scrape Details")

    return details

# --- Main Processing Function for the Excel File ---

# MODIFIED: Handle 'Scraping Issues' column
def process_excel_for_details(driver: WebDriver, config: dict):
    """Reads Excel, scrapes details, updates DataFrame including 'Scraping Issues', and saves."""
    # --- Config Extraction ---
    excel_filepath = config['paths']['excel_filepath']
    status_new = config['status']['NEW']
    status_processing = config['status']['PROCESSING_DETAILS']
    status_ready_for_ai = config['status']['READY_FOR_AI']
    status_invalid_link = config['status']['INVALID_LINK']
    status_scrape_failed = config['status']['FAILED_SCRAPE_DETAILS']
    phase2_error_statuses = [status_scrape_failed, config['status'].get('FAILED_WEBDRIVER'), config['status'].get('MISSING_DATA'), status_invalid_link]
    phase2_error_statuses = [s for s in phase2_error_statuses if s]

    save_interval = config['phase2']['save_interval']
    retry_failed = config['workflow']['retry_failed_phase2']

    logging.info(f"Starting detailed scraping process for Excel file: {excel_filepath}")
    logging.info(f"Retry previously failed rows: {retry_failed}")

    try:
        if not os.path.exists(excel_filepath):
            logging.error(f"Input Excel file not found: '{excel_filepath}'. Phase 2 cannot run.")
            return False

        logging.info("Reading Excel file...")
        df = pd.read_excel(excel_filepath, engine='openpyxl', dtype=object)
        logging.info(f"Read {len(df)} rows from Excel.")

        # --- Schema Check & Update ---
        added_cols = False
        current_columns = list(df.columns)
        # Use updated ALL_EXPECTED_COLUMNS (implicitly includes 'Scraping Issues')
        for col in ALL_EXPECTED_COLUMNS:
             if col not in df.columns:
                  logging.warning(f"Adding missing column '{col}' to DataFrame.")
                  df[col] = '' # Initialize new cols as empty string
                  added_cols = True
        if added_cols or list(df.columns) != ALL_EXPECTED_COLUMNS:
             logging.info("Reordering DataFrame columns to standard order.")
             # Fill value ensures new cols have empty string instead of NaN
             df = df.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='')

        # --- Fill NA/NaN in Crucial Columns ---
        df['Status'] = df['Status'].fillna(status_new)
        df['Link'] = df['Link'].fillna('')
        # Fill NA in other text columns potentially used
        for col in ['Title', 'Company', 'Notes', 'Scraping Issues']:
             if col in df.columns: df[col] = df[col].fillna('')

        # --- Filter rows to process ---
        statuses_to_process = [status_new]
        if retry_failed:
            statuses_to_process.extend(phase2_error_statuses)
            statuses_to_process = list(set(statuses_to_process))
        logging.info(f"Will process jobs with status in: {statuses_to_process}")

        rows_to_process_mask = df['Status'].isin(statuses_to_process)
        rows_to_process_idx = df[rows_to_process_mask].index
        num_to_process = len(rows_to_process_idx)
        logging.info(f"Found {num_to_process} rows matching processing criteria.")

        if num_to_process == 0:
            logging.info("No jobs found to process in this phase.")
            if added_cols: # Save if schema changed
                try:
                    logging.info("Saving Excel file due to schema changes...")
                    df.to_excel(excel_filepath, index=False, engine='openpyxl')
                except Exception as save_err: logging.error(f"Error saving schema changes: {save_err}"); return False
            return True

        # --- Processing Loop ---
        update_count = 0
        processed_in_run = 0
        batch_start_time = time.time()

        for index in rows_to_process_idx:
            processed_in_run += 1
            job_link = df.loc[index, 'Link']
            job_Company = df.loc[index, 'Company']
            job_title = df.loc[index, 'Title']

            logging.info(f"--- Processing Row {index + 1}/{len(df)} (Index: {index}) | Job: '{job_title}' | Company: '{job_Company}' ---")

            if pd.isna(job_link) or not isinstance(job_link, str) or not job_link.strip().startswith('http'):
                logging.warning(f"Skipping row {index + 1} due to invalid/missing link: '{job_link}'")
                df.loc[index, 'Status'] = status_invalid_link
                continue

            # Reset scraping issues for the current row before processing
            df.loc[index, 'Scraping Issues'] = ''
            df.loc[index, 'Status'] = status_processing # Set status BEFORE scraping

            try:
                scraped_details = scrape_job_details(driver, job_link, config, job_title, job_Company)

                # Update DataFrame with scraped details
                if scraped_details.get('_scrape_successful', False): # Check internal flag first
                    for col, value in scraped_details.items():
                        if col in df.columns and not col.startswith('_'):
                            df.loc[index, col] = value # This now includes 'Scraping Issues'
                    df.loc[index, 'Status'] = status_ready_for_ai
                    update_count += 1
                    logging.info(f"Successfully processed row {index+1}. Status: '{status_ready_for_ai}'.")
                    # Log issues if any were recorded
                    if scraped_details.get('Scraping Issues'):
                         logging.warning(f"  Row {index+1} scraping issues: {scraped_details['Scraping Issues']}")
                else:
                     # Handle critical failure from scrape_job_details
                     error_msg = scraped_details.get('_error_message', status_scrape_failed)
                     df.loc[index, 'Status'] = error_msg
                     logging.error(f"Scraping failed critically for row {index+1}. Status: '{df.loc[index, 'Status']}'. Link: {job_link}")
                     if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"Phase 2 Critical Error: {error_msg}"

                # --- Periodic Save ---
                if processed_in_run % save_interval == 0:
                    batch_time = time.time() - batch_start_time
                    logging.info(f"Processed {processed_in_run} rows ({batch_time:.2f} sec). Saving progress...")
                    try:
                        # Fill NA before saving for cleaner Excel
                        df_save_prog = df.fillna('')
                        df_save_prog.to_excel(excel_filepath, index=False, engine='openpyxl')
                        logging.info("Progress saved successfully.")
                        batch_start_time = time.time()
                    except PermissionError: logging.error(f"PERM ERROR saving progress: {excel_filepath}. PLEASE CLOSE THE FILE. Stopping phase."); return False
                    except Exception as save_err: logging.error(f"Error saving progress: {save_err}", exc_info=True); logging.warning("Continuing processing...")

                delay = get_random_delay(config, "long")
                logging.debug(f"Waiting {delay:.2f} seconds before next job...")
                time.sleep(delay)

            except WebDriverException as wd_exc:
                 # Catch critical WebDriver error from scrape_job_details
                 logging.critical(f"WebDriverException during processing row {index+1}. Stopping phase.")
                 df.loc[index, 'Status'] = config['status'].get("FAILED_WEBDRIVER", "Error - WebDriver Exception")
                 if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"WebDriverException: {str(wd_exc)[:200]}"
                 # Attempt final save before returning False
                 try: df.fillna('').to_excel(excel_filepath, index=False, engine='openpyxl')
                 except Exception as final_save_err: logging.error(f"Error during final save attempt: {final_save_err}")
                 return False # Critical failure
            except Exception as row_err:
                 logging.error(f"Unexpected error processing row {index+1} for link {job_link}: {row_err}", exc_info=True)
                 df.loc[index, 'Status'] = config['status'].get("UNKNOWN_ERROR", "Error - Unknown")
                 if 'Notes' in df.columns: df.loc[index, 'Notes'] = f"Phase 2 Loop Error: {str(row_err)[:250]}"
                 continue # Continue to the next row

        # --- Final Save After Loop ---
        logging.info("Finished processing all designated rows. Performing final save...")
        try:
             # Reindex and fillna before final save
             df_final = df.reindex(columns=ALL_EXPECTED_COLUMNS, fill_value='').fillna('')
             df_final.to_excel(excel_filepath, index=False, engine='openpyxl')
             logging.info("Final Excel file saved successfully.")
        except PermissionError: logging.error(f"FINAL SAVE ERROR: Permission denied: {excel_filepath}."); return False
        except Exception as save_err: logging.error(f"Error during final save: {save_err}", exc_info=True); return False

        logging.info(f"Phase 2 finished. Successfully processed {update_count} out of {num_to_process} targeted rows (check logs for warnings on optional fields).")
        return True

    except FileNotFoundError: logging.error(f"Excel file not found at start: '{excel_filepath}'."); return False
    except KeyError as e: logging.error(f"Missing key in config/DataFrame during setup: {e}", exc_info=True); return False
    except Exception as e: logging.critical(f"Crit error during Phase 2 setup: {e}", exc_info=True); return False

# --- Main Function for Phase 2 ---
def run_phase2_detail_scraping(config: dict) -> bool:
    """Executes Phase 2: connect Selenium, read Excel, scrape details, update status/issues, save."""
    logging.info("Initiating Phase 2: Job Detail Scraping")
    driver = None
    overall_success = False

    # Use the enhanced setup function (imported from phase1_list_scraper)
    driver = setup_selenium_driver(config)

    if not driver:
        logging.critical("Failed to connect to Selenium WebDriver for Phase 2. Cannot proceed.")
        return False

    try:
        overall_success = process_excel_for_details(driver, config)
    except WebDriverException as e:
        logging.critical(f"WebDriverException caught in run_phase2: {e}")
        overall_success = False
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in run_phase2: {e}", exc_info=True)
        overall_success = False
    # Do NOT driver.quit() here if using debugger port

    if overall_success:
        logging.info("Phase 2 processing run completed.")
    else:
        logging.error("Phase 2 processing run finished with critical errors.")

    return overall_success

# No `if __name__ == "__main__":` block needed