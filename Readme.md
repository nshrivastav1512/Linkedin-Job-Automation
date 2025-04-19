# LinkedIn Job Search & Resume Tailoring Automaton 🚀

## Overview

This project automates the tedious process of searching for relevant jobs on LinkedIn, analyzing their requirements against your resume, and tailoring your resume specifically for high-match positions. It aims to streamline the job application workflow, save time, and increase the chances of landing interviews by submitting highly relevant resumes.

The core workflow involves:
1.  Scraping job listings from LinkedIn based on your search criteria.
2.  Extracting detailed information from each job posting.
3.  Utilizing Google's Gemini AI to analyze the job description, compare it against your resume, and generate a match score.
4.  For jobs exceeding a defined match score threshold, automatically tailoring your base HTML resume using AI suggestions.
5.  Generating both HTML and PDF versions of the tailored resumes.
6.  Storing all collected data and analysis results in a central Excel file for review and further analysis.

**Disclaimer:** Web scraping, especially on dynamic sites like LinkedIn, can be fragile. LinkedIn's website structure changes frequently, which may break the scraping components (Phases 1 & 2) of this tool. The CSS selectors defined in the configuration may need regular updates. Use this tool responsibly and ethically, respecting LinkedIn's Terms of Service. Automation should augment, not replace, genuine engagement.

## Key Features ✨

*   **Automated LinkedIn Job Scraping:** Fetches job listings based on keywords, location, and date filters.
*   **Detailed Data Extraction:** Gathers comprehensive job details, including descriptions, company info, and hiring team contacts (where available).
*   **AI-Powered Job Analysis:** Uses Google Gemini to:
    *   Extract key responsibilities, required/preferred skills, experience levels, and qualifications from job descriptions.
    *   Score the match between the job description and your resume (0-5 stars).
    *   Provide detailed feedback: Strengths, areas for improvement, and actionable recommendations.
*   **Automated Resume Tailoring:** For high-scoring jobs, uses AI to:
    *   Rewrite your resume summary.
    *   Adapt experience bullet points to highlight relevant achievements and keywords.
    *   Optimize the skills section based on job requirements.
*   **HTML & PDF Generation:** Creates tailored resumes in both HTML and PDF formats using WeasyPrint.
*   **Centralized Data:** Stores all scraped data, AI analysis, and file paths in a well-structured Excel spreadsheet.
*   **Configuration Driven:** Most parameters, including search terms, file paths, AI models, thresholds, and crucial CSS selectors, are managed via a central configuration file (`main_workflow.py`).
*   **Logging:** Comprehensive logging to both console and timestamped files for easy debugging and tracking.
*   **Workflow Control:** Ability to configure which phases of the workflow to run (e.g., skip scraping, only run analysis).
*   **Error Handling & Retries:** Includes options to retry processing jobs that failed in previous runs.

## Project Workflow / Phases 📝

The automation process is divided into distinct phases, orchestrated by `main_workflow.py`:

**Phase 0: Configuration & Setup (`main_workflow.py`)**
*   Loads all configuration settings (paths, credentials, search criteria, AI settings, selectors).
*   Initializes logging to console and file.
*   Loads the base `Resume.html` content and extracts plain text for AI analysis.
*   Calls the functions for the subsequent phases based on the `start_phase` and `end_phase` configuration.

**Phase 1: Scrape Job List (`phase1_list_scraper.py`)**
*   Connects to an existing Chrome instance running with remote debugging enabled.
*   Navigates to LinkedIn job search using configured search terms, location, and filters.
*   Scrapes basic information (Title, Company, Location, Link, Job ID, etc.) from job cards on the search results page(s).
*   Handles pagination using URL parameters (`&start=N`) primarily, with button clicks as a fallback.
*   Implements job limits (per page and total) if configured.
*   Adds newly found, unique jobs to the master Excel file (`linkedin_jobs_master_list.xlsx`) with an initial `Status` of `New`. Logs skipped duplicates. Creates the Excel file with all necessary columns if it doesn't exist.

**Phase 2: Scrape Job Details (`phase2_detail_scraper.py`)**
*   Connects to the Chrome instance.
*   Reads the master Excel file.
*   Filters for jobs with `Status` = `New` (or specific error statuses if `retry_failed_phase2` is True).
*   For each eligible job, navigates to its specific LinkedIn job `Link`.
*   Scrapes detailed information:
    *   Full Job Description (HTML and Plain Text)
    *   Company Link (from top card)
    *   Applicant Count, Posted Date (Detailed)
    *   "About Company" section details (Industry, Size, Followers, Description)
    *   Hiring Team members (if available)
*   Updates the corresponding row in the Excel file with the scraped details.
*   Updates the row `Status` to `Ready for AI` on success or an appropriate error status (e.g., `Error - Scrape Job Details`) on failure.
*   Saves the Excel file periodically.

**Phase 3: AI Analysis & Scoring (`phase3_ai_analysis.py`)**
*   Loads the Gemini API Key.
*   Reads the master Excel file.
*   Filters for jobs with `Status` = `Ready for AI` (or specific error statuses if `retry_failed_phase3` is True).
*   For each eligible job:
    *   **AI Call 1 (Extraction):** Sends the `Job Description Plain Text` to Gemini to extract structured data (`Extracted Responsibilities`, `Extracted Required Skills`, etc.).
    *   **AI Call 2 (Analysis):** Sends the extracted `Job Description Plain Text` and the plain text extracted from your base `Resume.html` to Gemini for comparison and scoring.
    *   Updates the corresponding row in the Excel file with the extracted data and the AI analysis results (`AI Match Score`, `AI Strengths`, `AI Areas for Improvement`, `AI Actionable Recommendations`, `AI Score Justification`).
    *   Updates the row `Status` to `AI Analyzed` on success or an appropriate error status (e.g., `Error - AI Extraction`, `Error - AI Analysis`) on failure.
*   Saves the Excel file periodically.

**Phase 4: AI Resume Tailoring & PDF Generation (`phase4_tailoring.py`)**
*   Loads the Gemini API Key.
*   Reads the master Excel file.
*   Filters for jobs where `Status` is `AI Analyzed` (or specific error/needs-edit statuses if `retry_failed_phase4` is True) AND `AI Match Score` meets or exceeds the configured `score_threshold`. Jobs below the threshold are marked as `Skipped - Low AI Score`.
*   For each eligible job:
    *   Copies the base `Resume.html` template to the `Tailored_Resumes` output folder with a unique filename.
    *   Constructs a detailed prompt for the Gemini tailoring model, providing the JD text, extracted skills/responsibilities (from Phase 3), company context (from Phase 2), AI analysis feedback (Strengths, Areas, Recommendations from Phase 3), and the base resume text (extracted from HTML).
    *   **Iterative Tailoring:**
        *   Calls Gemini to generate tailored text content (summary, bullets, skills) based on the prompt.
        *   Applies these AI suggestions to the copied HTML file using BeautifulSoup.
        *   Generates a PDF from the modified HTML using WeasyPrint.
        *   Validates the PDF page count.
        *   If > 1 page, constructs a new prompt asking the AI to condense the *previous* generated text and repeats the HTML edit -> PDF gen -> validation cycle (up to `max_tailoring_attempts`).
        *   If still > 1 page after max AI attempts, attempts a final manual edit (removing the last education bullet) and regenerates the PDF.
    *   Updates the corresponding row in the Excel file with the paths to the generated HTML and PDF files, the *cleaned* (tags stripped) tailored text content, and the final `Status` (`Tailored Resume Created`, `Tailored Needs Manual Edit (Length)`, or a specific failure status).
*   Saves the Excel file periodically.

## Technology Stack 💻

*   **Language:** Python 3.10+
*   **Web Scraping:** Selenium (with ChromeDriver)
*   **HTML Parsing:** BeautifulSoup4
*   **AI Model:** Google Gemini API (Flash & Pro models recommended)
*   **Data Handling:** Pandas
*   **Excel Interaction:** Openpyxl
*   **PDF Generation:** WeasyPrint
*   **Environment Management:** python-dotenv
*   **PDF Reading (for validation):** PyPDF2

## Prerequisites 📋

1.  **Python:** Version 3.10 or higher recommended. [Download Python](https://www.python.org/downloads/)
2.  **Pip:** Python package installer (usually comes with Python).
3.  **Git:** For cloning the repository. [Download Git](https://git-scm.com/downloads)
4.  **Google Chrome:** The script uses Selenium with ChromeDriver. [Download Chrome](https://www.google.com/chrome/)
5.  **ChromeDriver:** Needs to be downloaded separately and **must match your installed Chrome version**. [Download ChromeDriver](https://googlechromelabs.github.io/chrome-for-testing/)
6.  **LinkedIn Account:** You need an active LinkedIn account to perform searches. The script assumes you will manually log in via the Chrome debugging instance.
7.  **Google Gemini API Key:** You need an API key for the Gemini models. Obtain one from Google AI Studio or Google Cloud Console. [Get Gemini API Key](https://aistudio.google.com/app/apikey)
8.  **(OS Dependent)** **WeasyPrint System Dependencies:** WeasyPrint relies on external libraries (Pango, Cairo, GObject, libffi) for rendering. Installation varies by OS:
    *   **Windows:** Requires installing GTK+ runtime libraries. See WeasyPrint Windows documentation or use unofficial installers (like from MSYS2 or `pip install weasyprint[windows]`). Ensure the GTK+ DLLs are in your system's PATH.
    *   **macOS:** Use Homebrew: `brew install pango cairo libffi`
    *   **Linux (Debian/Ubuntu):** `sudo apt-get update && sudo apt-get install python3-dev python3-pip python3-setuptools python3-wheel python3-cffi libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info`
    *   Refer to the official [WeasyPrint Installation Docs](https://doc.courtbouillon.org/weasyprint/stable/first_steps.html#installation) for details.

## Setup Instructions 🛠️

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/YOUR_USERNAME/YOUR_REPOSITORY_NAME.git
    cd YOUR_REPOSITORY_NAME
    ```

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    # Activate it:
    # Windows (cmd/powershell):
    venv\Scripts\activate
    # macOS/Linux (bash/zsh):
    source venv/bin/activate
    ```

3.  **Install Python Dependencies:**
    *(First, ensure you have created a `requirements.txt` file containing the necessary libraries)*
    ```
    # Example requirements.txt contents:
    pandas
    openpyxl
    selenium
    beautifulsoup4
    python-dotenv
    google-generativeai
    weasyprint
    pypdf2
    requests
    ```
    Then run:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Setup ChromeDriver:**
    *   Download the ChromeDriver version that **matches your installed Google Chrome browser version** from [ChromeDriver Downloads](https://googlechromelabs.github.io/chrome-for-testing/).
    *   Place the `chromedriver.exe` (or `chromedriver` on macOS/Linux) executable either:
        *   In a directory included in your system's **PATH** environment variable.
        *   OR update the absolute path in `main_workflow.py` under `CONFIG_SELENIUM['chromedriver_path']`.

5.  **Setup WeasyPrint Dependencies (OS Specific):**
    *   Follow the instructions in the **Prerequisites** section above for your operating system (Windows, macOS, or Linux) to install libraries like GTK+, Pango, Cairo, etc. This is crucial for PDF generation in Phase 4.

6.  **Create `.env` File:**
    *   In the root directory of the project, create a file named exactly `.env`.
    *   Add your Google Gemini API key to this file:
        ```dotenv
        GEMINI_API_KEY=YOUR_ACTUAL_API_KEY_HERE
        ```
    *   Replace `YOUR_ACTUAL_API_KEY_HERE` with your real key.

7.  **Prepare `Resume.html`:**
    *   Ensure you have a well-structured HTML file named `Resume.html` in the root project directory. This file serves two purposes:
        *   Its *text content* is extracted and used by the Phase 3 AI for analysis.
        *   It acts as the *base template* that Phase 4 modifies to create tailored versions.
    *   Make sure the HTML has clear semantic structure (e.g., using `<h2>` for sections like "Summary", "Experience", "Skills"; using `<h3>` for job titles within Experience; using `<ul>` and `<li>` for bullet points) as the Phase 4 `edit_html_with_ai_suggestions` function relies on this structure. Refer to the provided `Resume.html` example.

8.  **Review Configuration:**
    *   Open `main_workflow.py`.
    *   Carefully review and adjust the settings within the `CONFIG_` dictionaries, especially:
        *   `CONFIG_PATHS`: Verify all file paths are correct relative to `main_workflow.py`.
        *   `CONFIG_SELENIUM['chromedriver_path']`: Double-check if not using PATH.
        *   `CONFIG_SELENIUM['debugger_port']`: Ensure it matches how you'll launch Chrome.
        *   `CONFIG_WORKFLOW`: Set the `start_phase` and `end_phase` for your desired run. Set `retry_failed_...` flags if needed.
        *   `CONFIG_PHASE1`: Set your desired `search_term`, `search_location_text`, `date_filter_choice`, page/job limits.
        *   `CONFIG_AI`: Verify model names (Flash vs Pro can impact cost/quality/speed). Adjust `api_delay_seconds` if you encounter rate limits.
        *   `CONFIG_PHASE4`: Set the `score_threshold` to control which jobs trigger tailoring.
        *   `CONFIG_LINKEDIN_SELECTORS`: **Be prepared to update these if scraping fails!**

## Configuration Details (`main_workflow.py`) ⚙️

All configuration is centralized in `main_workflow.py`.

*   **`CONFIG_PATHS`**: Defines essential file and folder locations.
    *   `excel_filepath`: The master spreadsheet where all data is stored. (MANDATORY)
    *   `resume_html_filepath`: Path to your base HTML resume template. (MANDATORY)
    *   `output_folder`: Where tailored HTML/PDF resumes are saved. (MANDATORY)
    *   `log_folder`: Where log files are stored. (MANDATORY)
    *   `env_filepath`: Path to your `.env` file for API keys. (MANDATORY)
*   **`CONFIG_SELENIUM`**: Settings for Selenium WebDriver.
    *   `chromedriver_path`: Full path to your ChromeDriver executable. (MANDATORY)
    *   `debugger_port`: Port for Chrome remote debugging (must match Chrome launch command). (MANDATORY)
    *   `wait_time_short`/`long`: Timeouts (in seconds) for waiting for elements. (OPTIONAL)
    *   `enable_random_delays`, `delay_*_base`, `delay_*_variance`: Control randomized pauses between actions to mimic human behavior. (OPTIONAL)
*   **`CONFIG_WORKFLOW`**: Controls the execution flow.
    *   `start_phase`: The first phase number to run (1-4). (MANDATORY)
    *   `end_phase`: The last phase number to run (1-4). (MANDATORY)
    *   `retry_failed_phaseX`: Set to `True` to re-process rows that failed in the specified phase during a previous run. (OPTIONAL, default False)
*   **`CONFIG_PHASE1`**: Settings specific to job list scraping.
    *   `search_term`: Job title/keywords. (MANDATORY)
    *   `search_location_text`: Location string. (OPTIONAL, leave `''` if not needed)
    *   `search_geo_id`: LinkedIn Geo ID. (OPTIONAL, leave `''` if not needed)
    *   `date_filter_choice`: '1' (Any), '2' (Month), '3' (Week), '4' (24h). (MANDATORY)
    *   `scrape_all_pages`: `True` to scrape multiple pages, `False` for first page only. (OPTIONAL)
    *   `max_pages_to_scrape`: Limit if `scrape_all_pages` is `True`. (OPTIONAL)
    *   `jobs_per_page_limit`: Max jobs to extract *per page*. 0 for no limit. (OPTIONAL)
    *   `total_jobs_limit`: Max jobs to extract *in total* for the Phase 1 run. 0 for no limit. (OPTIONAL)
    *   `save_after_each_page`: Save Excel after each page (slower but safer). (OPTIONAL)
    *   `verbose_card_extraction`: More detailed logging during card scraping. (OPTIONAL)
*   **`CONFIG_PHASE2`**: Settings specific to job detail scraping.
    *   `save_interval`: Save Excel every N jobs processed. (OPTIONAL)
*   **`CONFIG_AI`**: Settings for Google Gemini API interaction.
    *   `api_key_name`: Variable name for the API key in `.env`. (MANDATORY)
    *   `extraction_model_name`: Model used for Phase 3 data extraction. (MANDATORY)
    *   `analysis_model_name`: Model used for Phase 3 resume/JD analysis. (MANDATORY)
    *   `tailoring_model_name`: Model used for Phase 4 resume text generation. (MANDATORY)
    *   `api_delay_seconds`: Pause between API calls to avoid rate limits. (OPTIONAL)
    *   `safety_settings`: Configure content safety blocking levels. (OPTIONAL, review carefully)
    *   `generation_config_json`/`text`: Tuning parameters (temperature, max tokens) for AI responses. (OPTIONAL)
    *   `resume_html_filepath`: Path to the base HTML resume (used for text extraction). (MANDATORY)
*   **`CONFIG_PHASE4`**: Settings specific to resume tailoring.
    *   `score_threshold`: Minimum `AI Match Score` needed to trigger tailoring. (MANDATORY)
    *   `max_tailoring_attempts`: Max retries for the AI to fit the resume on one page. (OPTIONAL)
    *   `save_interval`: Save Excel every N resumes tailored. (OPTIONAL)
*   **`CONFIG_STATUS`**: Defines the standard strings used in the Excel 'Status' column to track progress. (Internal Use)
*   **`CONFIG_LINKEDIN_SELECTORS`**: **Crucial but fragile.** Contains CSS selectors used to find elements on LinkedIn pages. **These are the most likely settings to require updates if the script stops working**, especially after a LinkedIn website redesign. Use browser developer tools (Inspect Element) to find the correct selectors if needed.

## Running the Workflow ▶️

1.  **Start Chrome with Remote Debugging:**
    *   **IMPORTANT:** Close *all* other running instances of Google Chrome.
    *   Open your system's command line (Terminal on macOS/Linux, Command Prompt or PowerShell on Windows).
    *   Run the Chrome executable with the remote debugging flag, specifying the **same port** as configured in `CONFIG_SELENIUM['debugger_port']` (default is 9222). Using a separate user data directory is highly recommended:
        *   **Windows:** `"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\ChromeDebugProfile"` (Adjust path to `chrome.exe` if needed)
        *   **macOS:** `/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --user-data-dir="$HOME/ChromeDebugProfile"`
        *   **Linux:** `google-chrome --remote-debugging-port=9222 --user-data-dir="$HOME/ChromeDebugProfile"`
    *   A new Chrome window will open.

2.  **Log in to LinkedIn:**
    *   In the newly opened Chrome debugging window, navigate to `https://www.linkedin.com`.
    *   Log in to your LinkedIn account manually. Keep this window open.

3.  **Run the Python Script:**
    *   Open a *separate* terminal or command prompt window.
    *   Navigate to the project directory (`cd YOUR_REPOSITORY_NAME`).
    *   Activate your virtual environment (e.g., `source venv/bin/activate` or `venv\Scripts\activate`).
    *   Execute the main workflow script:
        ```bash
        python main_workflow.py
        ```

4.  **Monitor:**
    *   Watch the console output for real-time progress and any error messages.
    *   Check the timestamped log file created in the `logs/` directory for more detailed information.
    *   Observe the Chrome window being controlled by Selenium.

## Project File Structure 📁

```markdown
YOUR_REPOSITORY_NAME/
├── .env                       # Stores API keys (!! Add to .gitignore !!)
├── main_workflow.py           # Phase 0: Main script, configuration, orchestration
├── phase1_list_scraper.py     # Phase 1: Job list scraping logic
├── phase2_detail_scraper.py   # Phase 2: Job detail scraping logic
├── phase3_ai_analysis.py      # Phase 3: AI analysis and scoring logic
├── phase4_tailoring.py        # Phase 4: Resume tailoring and PDF logic
├── Resume.html                # Your base HTML resume template
├── linkedin_jobs_master_list.xlsx # Output Excel file (created on first run)
├── requirements.txt           # List of Python dependencies
├── logs/                      # Folder for log files (created on first run)
│   └── log_YYYYMMDD_HHMMSS_SearchTerm_Location.log # Example log file
└── Tailored_Resumes/          # Folder for output resumes (created by Phase 4)
    ├── Company_JobTitle_ID.html # Example tailored HTML resume
    └── Company_JobTitle_ID.pdf  # Example tailored PDF resume
```

**Important:** Add `.env` and potentially `logs/`, `Tailored_Resumes/`, `*.xlsx`, and `venv/` (if you use one) to your `.gitignore` file before committing to a public repository to avoid exposing secrets or large generated files.

## Output Explanation 📊📄

*   **`linkedin_jobs_master_list.xlsx`:** This is the central data store.
    *   Rows are added/updated by Phases 1, 2, 3, and 4.
    *   Contains scraped data (job title, company, link, description, etc.).
    *   Contains extracted data from AI (skills, responsibilities, etc.).
    *   Contains AI analysis results (match score, feedback).
    *   Contains paths to tailored resume files.
    *   The `Status` column shows the last completed step for each job.
*   **`logs/` Folder:** Contains detailed log files for each run, named with timestamp, search term, and location. Useful for debugging errors.
*   **`Tailored_Resumes/` Folder:** Contains the output of Phase 4. For each job that meets the score threshold:
    *   An `.html` file with the AI-modified resume content.
    *   A `.pdf` file generated from the tailored HTML.

## Error Handling & Troubleshooting ⚠️

*   **Selenium Connection Error:** Ensure Chrome is running with the correct `--remote-debugging-port` and that the port number matches `CONFIG_SELENIUM['debugger_port']`. Close all other Chrome instances before starting the debug instance.
*   **ChromeDriver Error:** Make sure your downloaded ChromeDriver version exactly matches your Chrome browser version. Ensure the `chromedriver_path` in the config is correct or that ChromeDriver is in your system PATH.
*   **LinkedIn Selector Errors (Timeout / NoSuchElement in Phase 1 or 2):** LinkedIn has likely updated its website structure.
    1.  Manually navigate to the relevant LinkedIn page (search results or job details).
    2.  Use your browser's Developer Tools (Right-click -> Inspect).
    3.  Find the HTML element that the script failed to locate.
    4.  Identify a more stable selector (e.g., using IDs if available, `data-*` attributes, less specific class names, or structural relationships).
    5.  Update the corresponding selector value in the `CONFIG_LINKEDIN_SELECTORS` dictionary in `main_workflow.py`.
*   **Phase 3 `AttributeError: ... FinishReason` / API Errors:** Ensure you have the latest `google-generativeai` library (`pip install -U google-generativeai`). Verify your API key in `.env` is correct and active. Check Google Cloud Console for any API usage limits or billing issues. Reduce the frequency of calls by increasing `api_delay_seconds`.
*   **Phase 4 WeasyPrint Errors (PDF Generation):** Usually indicates missing system dependencies (GTK+/Pango/Cairo/libffi). Revisit the **Prerequisites** section and ensure the OS-specific dependencies for WeasyPrint are correctly installed.
*   **PermissionError (Saving Excel/Files):** Make sure the Excel file (`linkedin_jobs_master_list.xlsx`) is not open in Excel or another program when the script is running. Check folder permissions for `logs/` and `Tailored_Resumes/`.
*   **FileNotFoundError (Excel, Resume.html, .env):** Verify the file paths configured in `CONFIG_PATHS` within `main_workflow.py` are correct and the files exist in the expected locations relative to `main_workflow.py`.

## Future Enhancements / Roadmap 💡

*   **Phase 2.5 - Company Detail Scraping:** Add a dedicated phase to visit company LinkedIn pages and scrape richer company information (website, HQ, full description, specialties).
*   **Support for Other Job Boards:** Create separate, dedicated modules for scraping and processing jobs from Indeed, Naukri, etc., potentially sharing core components (Excel, AI analysis, tailoring).
*   **Improved AI Prompts:** Continuously refine the prompts used in Phase 3 and Phase 4 for better extraction, analysis, and tailoring results.
*   **More Sophisticated HTML Editing:** Enhance the `edit_html_with_ai_suggestions` function to handle more complex structural changes if needed.
*   **User Interface:** Develop a simple GUI (e.g., using Tkinter, PyQt, or a web framework like Flask/Streamlit) for easier configuration and execution.
*   **Database Storage:** Replace the Excel file with a database (like SQLite or PostgreSQL) for more robust data management, especially with a large number of jobs.
*   **Error Recovery:** Implement more granular error recovery within phases (e.g., retrying a specific job scrape after a short delay if a temporary network issue occurs).
