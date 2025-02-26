import requests
import sqlite3
import json
import time
from bs4 import BeautifulSoup 


def links_scrap(page_num, link_before="https://it.pracuj.pl/praca?pn=", link_after="&its=big-data-science%2Cai-ml"):   
    
    all_links = []

    URL = link_before + str(page_num) + link_after
    page = requests.get(URL)
    
    soup = BeautifulSoup(page.content, "html.parser")
    
    links = soup.find_all("a", class_="tiles_o1859gd9 core_n194fgoq") 
    
    for link in links:
        if "href" in link.attrs:
            all_links.append(link["href"])

    return all_links
    
def scrape_job_listing(url):
    """
    Scrapes a job listing page and returns a dictionary with parsed job details.
    
    The returned dictionary includes:
        - url: The URL of the job listing.
        - title: Job title from the <h1> element.
        - work_location: The location of the work/company.
        - validity: The validity period of the job offer.
        - contract_type: The type of contract/employment.
        - employment_type: Employment type (e.g., full-time).
        - position: The job position title.
        - work_arrangement: Work arrangement details (e.g., remote, hybrid).
        - start: Information on immediate employment.
        - recruitment_method: The method of recruitment.
        - additional_info: Any additional information.
        - technologies: List of technologies (from aggregate open dictionary model).
        - responsibilities: List of responsibilities (from the first aggregate bullet model).
        - requirements: List of requirements (from the second aggregate bullet model, if present).
        - application_link: The first application link found on the page.
    
    Parameters:
        url (str): The URL of the job listing page.
    
    Returns:
        dict: A dictionary with the scraped job details.
    """
    # Fetch the page content
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Error fetching the URL: {url} (Status code: {response.status_code})")
    
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Initialize lists to hold various benefit details
    sections_benefit_list = []
    aggregate_open_dictionary_model = []
    aggregate_bullet_model_1 = []
    aggregate_bullet_model_2 = []
    
    # Extract all "aggregate-bullet-model" lists (these may occur in multiple locations)
    aggregate_bullet_models = soup.find_all('ul', {'data-test': 'aggregate-bullet-model'})
    if len(aggregate_bullet_models) > 0:
        aggregate_bullet_model_1 = [li.get_text(strip=True) for li in aggregate_bullet_models[0].find_all('li')]
    if len(aggregate_bullet_models) > 1:
        aggregate_bullet_model_2 = [li.get_text(strip=True) for li in aggregate_bullet_models[1].find_all('li')]
    
    # Extract additional lists: "sections-benefit-list" and "aggregate-open-dictionary-model"
    data_lists = soup.find_all('ul', {'data-test': ['sections-benefit-list', 'aggregate-open-dictionary-model']})
    for data_list in data_lists:
        list_type = data_list.get('data-test')
        items = [li.get_text(strip=True) for li in data_list.find_all('li')]
        if list_type == 'sections-benefit-list':
            sections_benefit_list.extend(items)
        elif list_type == 'aggregate-open-dictionary-model':
            aggregate_open_dictionary_model.extend(items)
    
    # Extract all links with the specific class "b14qiyz3"
    links_list = [a.get('href') for a in soup.find_all('a', class_='b14qiyz3')]
    
    def parse_benefit_list(benefit_list):
        """
        Parses a list of benefit strings and returns a dictionary with job attributes.
        
        The returned dictionary includes:
            - work_location: Work or company location.
            - validity: Validity period of the job offer.
            - contract_type: Type of contract or agreement.
            - employment_type: Employment type (e.g., full-time).
            - position: Job position title.
            - work_arrangement: Details on work arrangement (e.g., remote or hybrid).
            - start: Immediate employment information.
            - recruitment_method: Method of recruitment.
            - additional_info: Any other information.
        """
        parsed = {
            "work_location": None,
            "validity": None,
            "contract_type": None,
            "employment_type": None,
            "position": None,
            "work_arrangement": None,
            "start": None,
            "recruitment_method": None,
            "additional_info": []
        }
        
        for item in benefit_list:
            lower_item = item.lower()
            # Work location details
            if ("siedziba firmy" in lower_item or "company location" in lower_item or 
                "miejsce pracy" in lower_item or ("work location" in lower_item and parsed["work_location"] is None)):
                parsed["work_location"] = item
            # Validity period
            elif "valid for" in lower_item or "ważna jeszcze" in lower_item:
                parsed["validity"] = item
            # Contract type
            elif "b2b" in lower_item or "kontrakt" in lower_item or "umowa" in lower_item:
                parsed["contract_type"] = item
            # Employment type
            elif "full-time" in lower_item or "pełny etat" in lower_item:
                parsed["employment_type"] = item
            # Job position title
            elif "specialist" in lower_item or "specjalista" in lower_item:
                parsed["position"] = item
            # Work arrangement (e.g., hybrid, remote)
            elif ("hybrid" in lower_item or "home office" in lower_item or 
                  "praca zdalna" in lower_item or "praca hybrydowa" in lower_item):
                parsed["work_arrangement"] = item
            # Immediate start information
            elif "immediate" in lower_item or "od zaraz" in lower_item:
                parsed["start"] = item
            # Recruitment method
            elif "rekrutacja" in lower_item or "recruitment" in lower_item:
                parsed["recruitment_method"] = item
            # Any additional information
            else:
                parsed["additional_info"].append(item)
        
        if not parsed["position"]:
            parsed["position"] = None
        if not parsed["additional_info"]:
            parsed["additional_info"] = None
        
        return parsed
    
    # Parse the benefit list to extract job attributes
    job_data = parse_benefit_list(sections_benefit_list)
    
    # Add additional scraped data to the job_data dictionary
    job_data["technologies"] = aggregate_open_dictionary_model
    job_data["responsibilities"] = aggregate_bullet_model_1
    job_data["requirements"] = aggregate_bullet_model_2  # Use second bullet model if available
    job_data["application_link"] = links_list[0] if links_list else None
    
    # Extract the job title from the <h1> element with data-test "text-positionName"
    job_title_element = soup.find('h1', {'data-test': 'text-positionName'})
    job_data["title"] = job_title_element.get_text(strip=True) if job_title_element else None
    
    # Include the URL in the job data dictionary
    job_data["url"] = url
    
    return job_data   
    
def create_db(db_name="jobs.db"):
    """
    Creates a SQLite database with a table for job records.
    
    The table has columns matching the job record dictionary keys.
    List values are stored as JSON strings.
    
    Parameters:
        db_name (str): Name of the SQLite database file.
    
    Returns:
        sqlite3.Connection: The database connection.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            title TEXT,
            work_location TEXT,
            validity TEXT,
            contract_type TEXT,
            employment_type TEXT,
            position TEXT,
            work_arrangement TEXT,
            start TEXT,
            recruitment_method TEXT,
            additional_info TEXT,
            technologies TEXT,
            responsibilities TEXT,
            requirements TEXT,
            application_link TEXT
        )
    ''')
    conn.commit()
    return conn

def insert_job_record(conn, record):
    """
    Inserts a job record (as a dictionary) into the SQLite database.
    
    List values are converted to JSON strings.
    
    Parameters:
        conn (sqlite3.Connection): The database connection.
        record (dict): The job record dictionary.
    """
    cursor = conn.cursor()
    
    # Convert list values to JSON strings for storage.
    for key in ["additional_info", "technologies", "responsibilities", "requirements"]:
        if key in record and isinstance(record[key], list):
            record[key] = json.dumps(record[key])
    
    columns = ["url", "title", "work_location", "validity", "contract_type", 
               "employment_type", "position", "work_arrangement", "start", 
               "recruitment_method", "additional_info", "technologies", 
               "responsibilities", "requirements", "application_link"]
    
    values = [record.get(col) for col in columns]
    placeholders = ','.join(['?'] * len(columns))
    query = f"INSERT INTO job_records ({','.join(columns)}) VALUES ({placeholders})"
    cursor.execute(query, values)
    conn.commit()
    
def query_db(db_name="jobs.db"):
    """
    Queries the job_records table in the SQLite database and returns a list of records.
    
    List-type fields stored as JSON strings are converted back into Python objects.
    
    Parameters:
        db_name (str): The SQLite database file name.
        
    Returns:
        list: A list of dictionaries representing job records.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Execute a query to select all records from the job_records table.
    cursor.execute("SELECT * FROM job_records")
    rows = cursor.fetchall()
    
    # Get the column names from the cursor description.
    columns = [desc[0] for desc in cursor.description]
    
    records = []
    for row in rows:
        record = dict(zip(columns, row))
        # Convert JSON fields back to Python lists if they are not None.
        for key in ["additional_info", "technologies", "responsibilities", "requirements"]:
            if record.get(key):
                try:
                    record[key] = json.loads(record[key])
                except json.JSONDecodeError:
                    record[key] = record[key]
        records.append(record)
    
    conn.close()
    return records
    
def remove_duplicates(conn):
    """
    Removes duplicates from the job_records table, leaving only one entry for each combination of title, URL and application link.
    """
    cursor = conn.cursor()

    query = """
    DELETE FROM job_records
    WHERE id NOT IN (
        SELECT MIN(id) 
        FROM job_records 
        GROUP BY title, url, application_link
    );
    """

    try:
        cursor.execute(query)
        conn.commit()
        print("Duplicate records removed successfully.")
    except sqlite3.Error as e:
        print("Database error:", e)

def main(): 

    result = []
    for i in range(20):
        time.sleep(0.2)
        result += links_scrap(i)
    
    job_records = []
    
    for i in range(len(result)):
        time.sleep(0.2)
        record = scrape_job_listing(result[i])
        job_records.append(record)
        print(f"Record {i+1} added. Title: {record.get('title')}")

    # Save job records to the SQLite database
    conn = sqlite3.connect("jobs.db") # conn = create_db("jobs.db")
    for record in job_records:
        insert_job_record(conn, record)
    
    # Usuwamy duplikaty
    remove_duplicates(conn)

    conn.close()

if __name__ == "__main__":
    main()