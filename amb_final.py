import csv
from bs4 import BeautifulSoup
import requests

def save_to_csv(data, filename):
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Company', 'Industry', 'Location']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        for row in data:
            writer.writerow(row)


base_url = ('https://www.ambitionbox.com/list-of-companies?indianEmployeeCounts=1001-5000,501-1000&locations=mumbai,chennai,bengaluru&industries=electrical-equipment&sortBy=popular&page={}')
headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
}

page_numbers = range(1, 20)  # Example: Scrape pages 1 to 5
urls = [base_url.format(page) for page in page_numbers]

# Loop through each URL
for url in urls:
    # Send a GET request to the URL with headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all company cards
        company_cards = soup.find_all('div', class_='companyCardWrapper')

        filtered_data = []

        # Iterate through each company card
        for card in company_cards:
            # Extract information from the card
            company_name = card.find('h2', class_='companyCardWrapper__companyName').text.strip()
            other_details = card.find('span', class_='companyCardWrapper__interLinking').text.strip()

            # Split the other details based on '|' character
            details_list = other_details.split('|')

            # Extract industry and location
            industry = details_list[0].strip()
            location = details_list[-1].strip()
            if '+' in location:
                location = location.split('+')[0].strip()  # Remove "+and more" part

            filtered_data.append({
                'Company': company_name,
                'Industry': industry,
                'Location': location,
            })

        if filtered_data:
            for data in filtered_data:
                print("Company:", data['Company'])
                print("Industry:", data['Industry'])
                print("Location:", data['Location'])
                print("-" * 50)
            # Save filtered data to CSV
            filename = 'filtered_companies.csv'
            save_to_csv(filtered_data, filename)
            print("Data added to CSV:", filename)
            print(" ")
        else:
            print("No companies found matching the criteria.")
    else:
        print("Failed to retrieve the webpage. Status code:", response.status_code)