import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv

result = []
url = 'https://seolik.ru/user-agents-list'

r = requests.get(url)
soup = BeautifulSoup(r.text, 'html.parser')
table = soup.find('table')

for row in table.findAll('tr')[1:150]:
    col = row.findAll('td')
    agent_ = (col[1].getText()).replace('\n', '')
    if 'Gecko' not in agent_:
        continue
    result.append({
        'agent': agent_,
    })

result_data = pd.DataFrame(result)
result_data['agent'].to_csv('useragents.txt',
                            header=False, index=False, sep='\t',
                            quoting=csv.QUOTE_NONE)
