import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import time
import re
import numpy as np
import pickle
from collections import defaultdict
import logging

# Set up logging
logging.basicConfig(filename='dse_scraper.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# function to download files
def download_pdf(url, filename):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(response.content)
        logging.info(f"Downloaded {filename}")
    except requests.RequestException as e:
        logging.error(f"Failed to download {filename}: {str(e)}")

base_url = "https://dse.co.tz/"
try:
    response = requests.get(base_url, timeout=10)
    response.raise_for_status()
except requests.RequestException as e:
    logging.error(f"Failed to fetch website: {str(e)}")
    exit()

# Parse html content
soup = BeautifulSoup(response.text, 'lxml')
divs = soup.find_all('div', class_ = 'ms-2')

if not os.path.exists('reports'):
    os.makedirs('reports')
urls = []
for i, div in enumerate(divs,1):
    a_tag = div.find('a')
    if a_tag and 'href' in a_tag.attrs:
        url = a_tag['href']
        if url.startswith('/'):
            url = base_url.strip('/') + url
        elif not url.startswith('http'):
            url = base_url + url
        
        time.sleep(1)
        if re.search('daily',url):
            urls.append(url)

# Function to extract text from all divs with class starting with "c"
def extract_data_cells(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        data_cells = soup.find_all('div', class_=lambda x: x and x.startswith('c'))
        return [cell.get_text(strip=True) for cell in data_cells]
    except requests.RequestException as e:
        logging.error(f"Failed to extract data from {url}: {str(e)}")
        return []

# Cut text up to exchange rates
def get_cut_table(url):
    data = extract_data_cells(url)
    try:
        end_pos = [i for i,txt in enumerate(data) if re.search('TZS/GBP',txt)][0]
        return data[:end_pos+4]
    except IndexError:
        logging.error(f"Failed to find 'TZS/GBP' in data from {url}")
        return data

# Extract all data cells
data_all = [get_cut_table(url) for url in urls]

months = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
date_pattern = rf'(?ix)^(?:(?:\d{{1,2}}\W*)?{months}\W*\d{{2,4}}|{months}\W*-\W*{months}\W*\d{{2,4}}|\d{{1,2}}\W*{months}\W*\d{{2,4}})$'

def detect_headers(data):   
    head_indices = [i for i, item in enumerate(data) if re.match(date_pattern, item)]
    tmp = [head_indices[i+1]-head_indices[i] for i in range(len(head_indices)-1)]
    tmp_indices = [i+1 for i, diff in enumerate(tmp) if diff>1]
    tmp_indices.insert(0,0)
    x = [head_indices[i] for i in tmp_indices]
    logging.info(f'Table starting positions {x}')
    return x

def get_data_chunks(data):
    x = detect_headers(data)
    data_chunks = [data[x[i]:x[i+1]] for i in range(len(x)-1)] + [data[x[len(x)-1]:]]
    for chunk in data_chunks:
        chunk.insert(0,'indicator')
    return data_chunks

def convert_to_numeric(df, columns=None):
    if columns is None:
        columns = df.select_dtypes(include=['object']).columns
    for col in columns:
        df[col] = pd.to_numeric(df[col].str.replace(r',|\s', '', regex=True), errors='coerce') 
    return df

pattern_daily = rf'(?i)^(?:\d{{1,2}}\W*)?{months}\W*\d{{2,4}}'

def chunk2table(data):
    dfs = {}
    data_chunks = get_data_chunks(data)
    for i,_ in enumerate(data_chunks):
        try:
            ncols = len([s for s in data_chunks[i] if re.match(date_pattern,s)])+1
            tables = [data_chunks[i][j:j+ncols] for j in range(0, len(data_chunks[i]), ncols)]
            dfs[i] = pd.DataFrame(tables[1:],columns=tables[0])
            daily_cols =['indicator'] + [c for c in dfs[i] if re.search(pattern_daily,c)]
            dfs[i] = dfs[i].loc[dfs[i]['indicator']!='',daily_cols]
            dfs[i]['indicator'] = dfs[i]['indicator'].str.strip()
            dfs[i].index = dfs[i]['indicator']
            dfs[i].drop('indicator',axis=1 ,inplace=True)
            dfs[i] = dfs[i].transpose()
        except Exception as e:
            logging.error(f"Error in chunk2table for chunk {i}: {str(e)}")
    return dfs

dfs = chunk2table(data_all[0])

dfs_all = defaultdict(list)
for data in data_all:
    x = chunk2table(data)
    for k,v in x.items():
        dfs_all[k].append(v)

dfs_all = dict(dfs_all)
for j, _ in enumerate(dfs_all):
    dfs_all[j] = pd.concat([dfs_all[j][i] for i in range(len(dfs_all[j]))])

def split_string(string, part=1):
    if string is not None:
        result = re.split(r'(?<=\.\d{2})',string)
        if part == 1:
            return result[0] if len(result) > 0 else None
        elif part==2:
            return result[1] if len(result) > 1 else None

for i, _ in enumerate(dfs_all):
    try:
        dfs_all[i].index = pd.to_datetime(dfs_all[i].index, format="%d%b %y")
        dfs_all[i] = dfs_all[i][~dfs_all[i].index.duplicated(keep='first')]
        
        if 'Turnoverfrom SharesBoughtby' in dfs_all[i].columns:
            dfs_all[i] = dfs_all[i].rename(columns = {'Turnoverfrom SharesBoughtby': 'TurnoverfromSharesBoughtbyForeignInvestors', 
                                         'Turnoverfrom SharesSoldby':'TurnoverfromSharesSoldbyForeignInvestors'})
            dfs_all[i] = dfs_all[i][[c for c in dfs_all[i].columns if c!='ForeignInvestors:']]
        
        if 'FaceValueTransactionValue' in dfs_all[i].columns:
            dfs_all[i]['FaceValue'] = dfs_all[i]['FaceValueTransactionValue'].apply(split_string, part=1)
            dfs_all[i]['TransactionValue'] = dfs_all[i]['FaceValueTransactionValue'].apply(split_string, part=2)
            dfs_all[i] = dfs_all[i][['FaceValue', 'TransactionValue']]
    except Exception as e:
        logging.error(f"Error processing DataFrame {i}: {str(e)}")

for i,_ in enumerate(dfs_all):
    for col in dfs_all[i].columns:
        try:
            if pd.api.types.is_object_dtype(dfs_all[i][col]):
                dfs_all[i][col] = dfs_all[i][col].astype(str).str.replace(',', '')
            if not all(dfs_all[i][col].isna()):
                dfs_all[i][col] = pd.to_numeric(dfs_all[i][col], errors='coerce')
        except Exception as e:
            logging.error(f"Error converting column {col} in DataFrame {i}: {str(e)}")

try:
    if not os.path.exists('_existing.pkl'):
        with open('_existing.pkl', 'wb') as f:
            pickle.dump(dfs_all,f)
    else:       
        with open('_existing.pkl', 'rb') as file:
            existing = pickle.load(file)

    existing_update = []
    for i, _ in enumerate(existing):
        common_indices = set(existing[i].index) & set(dfs_all[i].index)
        existing[i] = existing[i].drop(common_indices)
        existing_update.append(pd.concat((existing[i], dfs_all[i])))
    logging.info('Data updated')

    with open('_existing.pkl', 'wb') as f:
        pickle.dump(existing_update,f)

    table_names = ['equity_market', 'govt_bonds', 'corporate_bonds', 'key_market_indicators', 'exchange_rate']
    for i, name in zip(range(len(existing_update)), table_names):
        try:
            os.makedirs('market_report', exist_ok=True)
            existing_update[i].to_csv(os.path.join('market_report',f'{name}.csv'), index=True)
            logging.info(f"Successfully wrote {name}.csv")
        except Exception as e:
            logging.error(f"Error writing {name}.csv: {str(e)}")
except Exception as e:
    logging.error(f"Error in final data processing and writing: {str(e)}")