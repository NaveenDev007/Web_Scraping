import sqlite3
def filter_data(name=None, date=None, location=None, keyword=None, pages=None):
    conn = sqlite3.connect('companies.db')
    cursor = conn.cursor()

    # Constructing the SQL query based on the provided filters
    query = "SELECT name, linkedin, date, location, description FROM companies WHERE 1=1"
    params = []

    if name:
        query += " AND LOWER(name) = ?"
        params.append(name.lower())

    if date:
        if len(date) == 4:  # If only year is provided
            query += " AND strftime('%Y', date) = ?"
            params.append(date)
        elif len(date) == 5:  # If month and day are provided (MM-DD)
            query += " AND strftime('%m-%d', date) = ?"
            params.append(date[2:])
        else:  # If complete year-month-date is provided (YYYY-MM-DD)
            query += " AND date = ?"
            params.append(date)

    if location:
        location_parts = [part.strip().lower() for part in location.split(',')]
        location_conditions = []
        for part in location_parts:
            location_conditions.append("LOWER(location) LIKE ?")
            params.append('%' + part + '%')

        query += " AND (" + " OR ".join(location_conditions) + ")"

    if keyword:
        keyword = keyword.lower()  # Convert keyword to lowercase
        query += " AND (LOWER(name) LIKE ? OR LOWER(description) LIKE ?)"
        params.extend(['%' + keyword + '%', '%' + keyword + '%'])

    query += " ORDER BY name"  # You can replace 'name' with any column you want to sort by

    if pages is not None and pages.strip():  # Check if pages is not empty
        if '-' in pages:  # Check if it's a range
            page_numbers = parse_page_range(pages)
        else:
            page_numbers = [int(pages)]  # Treat single value as a single page

        if page_numbers:
            # Fetch data for each page in the range
            data_list = []
            for page in page_numbers:
                page_data = fetch_page(cursor, query, params, page)
                data_list.extend(page_data)
            conn.close()
            return data_list

    # If pages is None, empty, or invalid, fetch all data
    cursor.execute(query, params)
    rows = cursor.fetchall()
    # Convert rows to list of dictionaries
    data_list = []
    for row in rows:
        data_list.append({
            'name': row[0],
            'linkedin': row[1],
            'date': row[2],
            'location': row[3],
            'description': row[4]
        })
    conn.close()
    return data_list


def parse_page_range(pages):
    try:
        start, end = map(int, pages.split('-'))
        return list(range(start, end + 1))
    except ValueError:
        return None


def fetch_page(cursor, query, params, page):
    page_size = 50
    offset = (page - 1) * page_size
    query_with_pagination = query + f" LIMIT ? OFFSET ?"
    params_with_pagination = params + [page_size, offset]
    cursor.execute(query_with_pagination, params_with_pagination)
    rows = cursor.fetchall()
    # Convert rows to list of dictionaries
    page_data = []
    for row in rows:
        page_data.append({
            'name': row[0],
            'linkedin': row[1],
            'date': row[2],
            'location': row[3],
            'description': row[4]
        })
    return page_data


def display_data(data_list):
    if not data_list:
        print("No results found.")
    else:
        for i, row in enumerate(data_list, start=1):
            print(f"\nResult {i}:")
            print('Company Name:', row['name'] if row['name'] else 'N/A')
            print('Linkedin:', row['linkedin'] if row['linkedin'] else 'N/A')
            print('Date:', row['date'] if row['date'] else 'N/A')
            print('Location:', row['location'] if row['location'] else 'N/A')
            print('Short Description:', row['description'] if row['description'] else 'N/A')
            print(' ')


def main():
    print("Welcome to Company Data Filtering!")
    print("Please enter your filter criteria:")
    name = input("Company Name: ")
    date = input("Date (YYYY-MM-DD): ")
    location = input("Location (or part of it): ")
    linkedin = input("Keyword: ")
    page = input("Page (leave empty to fetch all data or in range eg 1-5 to get 5 pages): ")

    # Filter the data from the database based on user input
    filtered_data = filter_data(name, date, location, linkedin, page)

    # Display the filtered data
    display_data(filtered_data)


if __name__ == "__main__":
    main()
