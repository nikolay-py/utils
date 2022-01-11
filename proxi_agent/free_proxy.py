import pandas as pd
import requests
from bs4 import BeautifulSoup

result = []
close_countries = ['Uzbekistan', 'Japan', 'Estonia',
                'Russian Federation', 'Germany',
                'Korea', 'Georgia', 'Turkey',
                'Poland', 'Uzbekistan', 'Ukraine',
                'Tajikistan', 'Norway', 'Bulgaria',
                'Belarus', 'Finland', 'Moldova, Republic of']

url = 'https://free-proxy-list.net/'
headers = {'User-Agent': ''}

r = requests.get(url, headers=headers)

soup = BeautifulSoup(r.text, 'html.parser')
table = soup.find('table')

for row in table.findAll('tr')[1:300]:
    col = row.findAll('td')
    result.append({
        'proxy': col[0].getText(),
        'port': col[1].getText(),
        'country': col[3].getText(),
        'http_available': col[6].getText(),
    }
    )

result_data = pd.DataFrame(result)
# print(result_data)
only_http = result_data.loc[result_data['http_available'] == 'yes']
close_country = only_http.loc[result_data['country'].isin(close_countries)]

only_http['proxy'].to_csv('proxies.txt', sep='\t', index=False, header=False)
# close_country['proxy'].to_csv('proxies.txt', sep='\t', index=False, header=False)
