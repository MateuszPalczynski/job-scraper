# it.pracuj.pl Job Scraper (Data Science / AI / Big Data)

This project is a Python-based web scraper designed to extract Data Science, AI, and Big Data job postings from the Polish job portal `it.pracuj.pl`.

It automates the process of fetching job links, scraping detailed information from each posting, and storing the structured data in an SQLite database, complete with duplicate removal.

---

## Features

* **Multi-Page Scraping:** Automatically fetches job links from multiple search result pages.
* **Bot Detection Evasion:** Uses `cloudscraper` to bypass basic bot-detection measures.
* **Detailed Parsing:** Extracts key job details, including title, technologies, responsibilities, requirements, and contract info, by parsing specific HTML `data-test` attributes.
* **Database Storage:** Saves all scraped data into a local `jobs.db` SQLite database.
* **Data Integrity:** Includes a function to remove duplicate job postings based on title, URL, and application link.
* **Structured Output:** Stores list-based data (e.g., technologies) as JSON strings in the database and provides a query function to retrieve and re-parse them.

---

## Technology Stack

* **Python 3**
* **`cloudscraper`**: For making HTTP requests that can bypass Cloudflare's anti-bot measures.
* **`BeautifulSoup4`**: For parsing HTML and extracting data.
* **`sqlite3`**: For local database storage.
* **`json`**: For serializing list data (technologies, requirements) for database storage.

---

## Workflow

The script's execution flow, as defined in the main (`if __name__ == "__main__":`) block, is as follows:

1.  **Initialize Database:** The `create_db` function (implicitly called by connecting) ensures the `jobs.db` file and `job_records` table exist.
2.  **Fetch Job Links:** The `links_scrap` function is called in a loop (for the first 20 pages) to gather all individual job posting URLs for the specified category.
3.  **Scrape Job Details:** The script iterates through the list of collected URLs. For each URL, `scrape_job_listing` is called.
    * `scrape_job_listing` fetches the page content.
    * It parses the HTML to find specific data points (technologies, responsibilities, etc.) based on `data-test` attributes.
    * It uses the `parse_benefit_list` helper to structure categorical data (like location, contract type).
    * It returns a complete dictionary for the job.
4.  **Store Records:** Each job dictionary is passed to `insert_job_record`, which converts lists to JSON and inserts the record into the database.
5.  **Remove Duplicates:** After all records are inserted, `remove_duplicates` is called to clean the database, ensuring only unique listings remain.
6.  **Query Data:** A separate `main` block demonstrates how to use the `query_db` function to fetch all processed records from the database and print them.

---
