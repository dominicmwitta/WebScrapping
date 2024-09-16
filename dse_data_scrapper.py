import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os

def scrape_and_append():
    url = 'https://dse.co.tz/#'
    response = requests.get(url)
    response = response.text

    # Get Table
    soup = BeautifulSoup(response, 'lxml')
    table = soup.find('table', class_="common_table tableScroll alt_row w-100")

    # Get headers
    headers = table.find_all('th')

    colnames = []
    for head in headers:
        name = head.text
        colnames.append(name)

    # Clean names
    colnames = [h.strip() for h in colnames]

    # Create dataframe
    df = pd.DataFrame(columns=colnames)

    # Get rows
    rows = table.find_all('tr')[1:]

    table_rows = []
    for r in rows:
        # Get data from rows
        data = r.find_all('td')
        row = [rw.text for rw in data]
        l = len(df)
        df.loc[l] = row

    date = soup.find('label').text
    trading_date = date.split()[-1]
    df['trading_date'] = trading_date

    # File to store the cumulative data
    path = 'daily_prices'
    os.makedirs(path)
    cumulative_file = os.path.join(path,'dse_daily_data.csv')

    # Check if the cumulative file exists
    if os.path.exists(cumulative_file):
        # Read existing data
        existing_df = pd.read_csv(cumulative_file)
        
        # Append new data
        updated_df = pd.concat([existing_df, df], ignore_index=True)
        
        # Remove duplicates based on all columns
        updated_df = updated_df.drop_duplicates()
    else:
        updated_df = df

    # Save the updated dataframe
    updated_df.to_csv(cumulative_file, index=False)
    print(f"Data appended to {cumulative_file}")

    # Optionally, you can also save today's data separately
    # today_file = f"dse_data_{datetime.date.today().strftime('%Y-%m-%d')}.csv"
    # df.to_csv(today_file, index=False)
    # print(f"Today's data also saved to {today_file}")

if __name__ == "__main__":
    scrape_and_append()


import logging
import traceback

# Set up logging
logging.basicConfig(filename='scraper_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_and_append():
    try:
        logging.info("Starting scrape_and_append function")
        # Your existing code here
        logging.info("Scrape and append completed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    logging.info("Script started")
    scrape_and_append()
    logging.info("Script finished")