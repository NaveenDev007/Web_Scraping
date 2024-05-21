import sqlite3
import requests
import numpy as np
import time
import threading

MAX_REQUESTS = 200  # Maximum number of requests allowed
TIME_WINDOW = 60  # Time window in seconds (1 minute in this example)
REQUESTS_MADE = 0  # Counter to track the number of requests made
LAST_REQUEST_TIME = 0  # Timestamp of the last request made
STOP_FLAG = False  # Flag to indicate whether code execution should stop

payload = {
    "field_ids": [
        "name",
        "location_identifiers",
        "short_description",
        "linkedin",
        "created_at",
        "rank_org"
    ],
    "order": [
        {
            "field_id": "rank_org",
            "sort": "asc"
        }
    ],
    "query": [
        {
            "type": "predicate",
            "field_id": "facet_ids",
            "operator_id": "includes",
            "values": [
                "company"
            ]
        }
    ],
    "limit": 1000,
    "after_id": ""
}

userkey = {"user_key": "48b5b46e87c2f24a6c7b8f404c976229"}


def company_count(payload):
    response = requests.post("https://api.crunchbase.com/api/v4/searches/organizations", params=userkey, json=payload)
    response.raise_for_status()
    result = response.json()
    total_companies = result["count"]
    return total_companies


def fetch_data(payload):
    global REQUESTS_MADE, LAST_REQUEST_TIME

    # Check if rate limit reached, wait if necessary
    while time.time() - LAST_REQUEST_TIME < TIME_WINDOW and REQUESTS_MADE >= MAX_REQUESTS:
        print("Rate limit reached. Waiting for next window...")
        time.sleep(65)  # Wait for 65 seconds
    if REQUESTS_MADE >= 200:
        REQUESTS_MADE = 0
    # Make API request
    url = "https://api.crunchbase.com/api/v4/searches/organizations?user_key=48b5b46e87c2f24a6c7b8f404c976229"
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    REQUESTS_MADE += 1
    LAST_REQUEST_TIME = time.time()

    if response.status_code == 429:
        # Rate limit exceeded, wait and retry
        retry_after = int(response.headers.get('Retry-After', 60))
        print(f"Rate limit exceeded. Waiting for {retry_after} seconds before retrying...")
        time.sleep(retry_after)
        return fetch_data(payload)

    # Check for HTTP errors
    response.raise_for_status()  # Raise error for non-200 status codes
    data = response.json()
    return data


def fetch_last_uuid(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT uuid FROM companies ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0]  # Return the last inserted UUID
    else:
        return None  # Return None if no UUID found


def fetch_last_uuid(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT uuid FROM companies ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    if result:
        return result[0]  # Return the last inserted UUID
    else:
        return None  # Return None if no UUID found


# Function to save data to the database
def save_data():
    global STOP_FLAG

    while not STOP_FLAG:
        conn = sqlite3.connect('companies.db')
        comp_count = company_count(payload)
        data_acq = 0

        while data_acq < comp_count:
            if data_acq != 0:
                last_uuid = fetch_last_uuid(conn)
                payload["after_id"] = last_uuid
                data = fetch_data(payload)
            else:
                data = fetch_data(payload)

            entities_list = data.get('entities', [])
            for entity in entities_list:
                uuid = entity.get("uuid")
                properties = entity.get('properties', {})
                name = properties.get('name', '')
                linkedin_dict = properties.get('linkedin', {})
                linkedin = linkedin_dict.get('value', '')
                date = properties.get('created_at', '')
                location_list = [loc.get('value', '') for loc in properties.get('location_identifiers', [])]
                location = ', '.join(location_list)
                description = properties.get('short_description', '')

                cursor = conn.cursor()
                cursor.execute("SELECT id, date FROM companies WHERE uuid=?", (uuid,))
                existing_entry = cursor.fetchone()
                if existing_entry:
                    existing_date = existing_entry[1]
                    if date and date > existing_date:
                        cursor.execute('''
                            UPDATE companies
                            SET name=?, linkedin=?, date=?, location=?, description=?
                            WHERE uuid=?
                        ''', (name, linkedin, date, location, description, uuid))
                        print(f"Updated entry with UUID: {uuid}")
                    else:
                        print(f"Skipping older entry with UUID: {uuid}")
                else:
                    cursor.execute('''
                        INSERT INTO companies (uuid, name, linkedin, date, location, description)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (uuid, name, linkedin, date, location, description))
                    print(f"Inserted new entry with UUID: {uuid}")

                conn.commit()
                cursor.close()
                data_acq += 1

            time.sleep(15)  # Wait for 60 seconds before fetching new data

        print("All data has been saved to the database.")
        conn.close()
        time.sleep(3600)  # Wait for 1 hour before fetching new data


def stop_code_execution():
    global STOP_FLAG
    input("Press Enter to stop the code execution...")
    STOP_FLAG = True


# Create SQLite connection and cursor
with sqlite3.connect('companies.db') as conn:
    cursor = conn.cursor()
    # Create table to store company data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY,
            uuid TEXT,
            name TEXT,
            linkedin TEXT,
            date DATE,
            location TEXT,
            description TEXT
        )
    ''')
    conn.commit()


# Start save_data function in a separate thread
save_data_thread = threading.Thread(target=save_data)
save_data_thread.start()

# Start stop_code_execution function in a separate thread
stop_thread = threading.Thread(target=stop_code_execution)
stop_thread.start()
