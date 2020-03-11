import os
import bs4
import requests
from tqdm import tqdm_notebook
import re
import pandas as pd

date_regex = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    }

def populate_db_folder():
    data = requests.get('http://insideairbnb.com/get-the-data.html', headers=headers).text
    soup = bs4.BeautifulSoup(data, 'html.parser')
    table_fr = soup.find('table', attrs={'class':'table table-hover table-striped paris'})
    urls_fr = [x['href'] for x in table_fr.find_all('a') if x['href'].endswith('/visualisations/listings.csv')]
    # urls_fr_2019 = [x for x in urls_fr if re.search(date_regex,x).group(1) == '2019']

    os.mkdir('db')
    for particle in tqdm_notebook(urls_fr, desc='1/3'):
        r = requests.get(particle)
        filename = re.search(date_regex, particle).group()
        with open(f'db\{filename}', 'wb') as f:
            f.write(r.content)


    df = pd.DataFrame()

    for part in tqdm_notebook(os.listdir('db'), desc='2/3'):
        tmp = pd.read_csv(f'db\{part}')
        tmp['period'] = part
        df = pd.concat([df,tmp], axis=0)

    df.to_csv(r'db\dataset.csv')
    for file in tqdm_notebook(os.listdir('db'), desc='3/3'):
        if file == 'dataset.csv': continue
        os.remove(f'db\{file}')
