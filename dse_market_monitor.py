import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import time
import re
import numpy as np

# function to download files
def download_pdf(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
            print(f"downloaded {filename}")
    else:
        print(f"Failed to download {filename}")

base_url = "https://dse.co.tz/"
response = requests.get(base_url)

if response.status_code !=200:
    print(f"Failed to fetch website with response code: {response.status_code}")
    exit()


# Parse html content
soup = BeautifulSoup(response.text, 'lxml')
divs = soup.find_all('div', class_ = 'ms-2')
# divs

if not os.path.exists('reports'):
    os.makedirs('reports')
urls = []
for i, div in enumerate(divs,1):
    a_tag = div.find('a')
    print(a_tag['href'])
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
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    data_cells = soup.find_all('div', class_=lambda x: x and x.startswith('c'))
    return [cell.get_text(strip=True) for cell in data_cells]

# Cut text up to exchange rates
def get_cut_table(url):
    data = extract_data_cells(url)
    end_pos = [i for i,txt in enumerate(data) if re.search('TZS/GBP',txt)][0]
    return data[:end_pos+4]

# Extract all data cells
# get data from all urls
data_all = [get_cut_table(url) for url in urls]



import re
import pandas as pd
months = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"

# # Regular expression pattern to match date-like strings with three-letter months
# date_pattern = rf'''
#         (?ix)  # Case-insensitive and verbose mode
#         ^(?:
#             (?:\d{{1,2}}\s?)?{months}\s?\d{{2,4}}|  # e.g., 11 Sep 24, Sep 2024, 23 Sept 24
#             {months}\s?-\s?{months}\s?\d{{2,4}}|  # e.g., Jul-Sep 24, Jul - Sep 2024
#             \d{{1,2}}\s?{months}\s?\d{{2,4}}  # e.g., 11 Sep 24, 11 Sep 2024
#         )$
#         '''

date_pattern = rf'(?ix)^(?:(?:\d{{1,2}}\W*)?{months}\W*\d{{2,4}}|{months}\W*-\W*{months}\W*\d{{2,4}}|\d{{1,2}}\W*{months}\W*\d{{2,4}})$'

def detect_headers(data):   
    # Find indices where new tables start
    head_indices = [i for i, item in enumerate(data)  if re.match(date_pattern, item)]
    # print(table_start_indices)

    tmp = [head_indices[i+1]-head_indices[i] for i in range(len(head_indices)-1)]
    tmp_indices = [i+1 for i, diff in enumerate(tmp) if diff>1]# Chexking if the difference is >1
    tmp_indices.insert(0,0)
    # x is table starting position
    x = [head_indices[i] for i in tmp_indices]
    print(f'Table starting positions {x}')
    return x


# slice data to obtain chunks
def get_data_chunks(data):
    x = detect_headers(data)
    data_chunks = [data[x[i]:x[i+1]] for i in range(len(x)-1)] +[data[x[len(x)-1]:]]

    # Insert name for 'indicators' 
    for chunk in data_chunks:
        chunk.insert(0,'indicator')
    return data_chunks


# data_chunks_all = []

def convert_to_numeric(df, columns=None):
    if columns is None:
        # If no columns specified, use all object columns
        columns = df.select_dtypes(include=['object']).columns
    
    for col in columns:
        # Replace all non-numeric characters and convert to numeric
        df[col] = pd.to_numeric(df[col].str.replace(r',|\s', '', regex=True), errors='coerce') 
    return df
#  cut data into separate tables
pattern_daily = rf'(?i)^(?:\d{{1,2}}\W*)?{months}\W*\d{{2,4}}'
# tables = {}
dfs = {}
def chunk2table(data):
    data_chunks = get_data_chunks(data)
    for i,_ in enumerate(data_chunks):
        ncols = len([s for s in data_chunks[i] if re.match(date_pattern,s)])+1
        # print(ncols)
        tables = [data_chunks[i][j:j+ncols] for j in range(0, len(data_chunks[i]), ncols)]
        # print(tables)
        dfs[i] = pd.DataFrame(tables[1:],columns=tables[0])

        # get the desired column only, for daily
        daily_cols =['indicator'] + [c for c in dfs[i] if re.search(pattern_daily,c)]
        dfs[i] = dfs[i].loc[dfs[i]['indicator']!='',daily_cols]
        # Strip whitespaces from indicator
        dfs[i]['indicator'] = dfs[i]['indicator'].str.strip()
        dfs[i].index = dfs[i]['indicator']
        dfs[i].drop('indicator',axis=1 ,inplace=True)
        # dfs[i] = convert_to_numeric(dfs[i])
        dfs[i] = dfs[i].transpose()
    return dfs
dfs = chunk2table(data_all[0])
print(dfs)
from collections import defaultdict


dfs_all = defaultdict(list)
for data in data_all:
    x = chunk2table(data)
    for k,v in x.items():
        dfs_all[k].append(v)

dfs_all = dict(dfs_all)
# dfs_all
for j, _ in enumerate(dfs_all):
    dfs_all[j] = pd.concat([dfs_all[j][i] for i in range(len(dfs_all[j]))])
dfs_all[1]


# split, conjoined cols
def split_string(string, part=1):
    if string is not None:
        result = re.split(r'(?<=\.\d{2})',string)
        if part == 1:
            return result[0]
        elif part==2:
            return result[1]

# Example  
# split_string('4.466789.04',1)


# do some cleaning
for i, _ in enumerate(dfs_all):
    # Convert index to datetime
    dfs_all[i].index = pd.to_datetime(dfs_all[i].index, format="%d%b %y")
    
    # Remove duplicates
    dfs_all[i] = dfs_all[i][~dfs_all[i].index.duplicated(keep='first')]
    # print(dfs_all[i])
    
    if 'Turnoverfrom SharesBoughtby' in dfs_all[i].columns:
        dfs_all[i] = dfs_all[i].rename(columns = {'Turnoverfrom SharesBoughtby': 'TurnoverfromSharesBoughtbyForeignInvestors', 
                                     'Turnoverfrom SharesSoldby':'TurnoverfromSharesSoldbyForeignInvestors'})
        dfs_all[i] = dfs_all[i][[c for c in dfs_all[i].columns if c!='ForeignInvestors:']]
        
        

    # Handle 'FaceValueTransactionValue' column if present
    if 'FaceValueTransactionValue' in dfs_all[i].columns:
        dfs_all[i]['FaceValue'] = dfs_all[i]['FaceValueTransactionValue'].apply(split_string, part=1)
        dfs_all[i]['TransactionValue'] = dfs_all[i]['FaceValueTransactionValue'].apply(split_string, part=2)
        dfs_all[i] = dfs_all[i][['FaceValue', 'TransactionValue']]
        print(f"DataFrame {i} columns after splitting: {dfs_all[i].columns}")
    
    # # Clean and convert columns to float

for i,_ in enumerate(dfs_all):
    for col in dfs_all[i].columns:
        if pd.api.types.is_object_dtype(dfs_all[i][col]):
            dfs_all[i][col] = dfs_all[i][col].astype(str).str.replace(',', '')
        # print(dfs_all[i])
        if not all(dfs_all[i][col].isna()):
            dfs_all[i][col] = pd.to_numeric(dfs_all[i][col], errors='coerce')
            # print(dfs_all[i].dtypes)
import pickle


if not os.path.exists('_existing.pkl'):
    with open('_existing.pkl', 'wb') as f:
        pickle.dump(dfs_all,f)
else:       
    with open('_existing.pkl', 'rb') as file:
        existing = pickle.load(file)
# common_indices = existing[0].index[existing[0].index.isin(dfs_all[0].index)]
# First, create the set of common indices, to identify duplicate dates
existing_update = []
for i, _ in enumerate(existing):
    common_indices = set(existing[i].index) & set(dfs_all[i].index)
    # Now, remove these common indices from existing
    existing[i] = existing[i].drop(common_indices)
    existing_update.append(pd.concat((existing[i], dfs_all[i])))
print('data updated')

with open('_existing.pkl', 'wb') as f:
    pickle.dump(existing_update,f)


# write to csv
table_names = ['equity_market', 'govt_bonds', 'corporate_bonds', 'key_market_indicators', 'exchange_rate']
for i, name in zip(range(len(existing_update)), table_names):
    existing_update[i].to_csv(os.path.join('market_report',f'{name}.csv'),index=True)
