from bs4 import BeautifulSoup
import requests
import pandas as pd
import logging
import numpy as np


class IndexHrScrape:

    def __init__(self, env):
        self.url = env.get('url')

    def get_page_content(self, pages):
        _list_page_content = []
        for page in range(1, pages):
            url = f'{self.url}{page}'
            r = requests.get(url=url)
            soup = BeautifulSoup(r.content, 'html.parser')
            apartments = soup.find_all('div', {'class': 'OglasiRezHolder'})

            for apartment in apartments:
                _dict = {}
                try:
                    _dict['Title'] = apartment.find('span', {'class': 'title px18'})\
                        .text.replace('\n', '').replace('\r', '').strip()
                except:
                    _dict['Title'] = None
                try:
                    _dict['Location'] = apartment.find('li', {'class': 'icon-marker'})\
                        .text.strip()
                except:
                    _dict['Location'] = None
                try:
                    _dict['PriceEuro'] = apartment.find('span', {'class': 'price'})\
                        .contents[0].text.replace(' €', '').partition(',')[0].strip()
                except:
                    _dict['PriceEuro'] = None
                try:
                    _dict['PriceKuna'] = apartment.find('span', {'class': 'price'})\
                        .contents[1].text.replace(' ~ ', '').replace(' kn', '').partition(',')[0].strip()
                except:
                    _dict['PriceKuna'] = None
                try:
                    _dict['Area'] = apartment.find('ul', {'class': 'tags hide-on-small-only'})\
                        .text.partition("m2")[2].partition('Adaptacija')[0].partition('Kat')[0]\
                        .replace('\r\n : ', '').replace('\r\n\n\r\n', '').replace('\r\n\n', '').partition('.')[0].strip()
                except:
                    _dict['Area'] = None
                try:
                    _dict['Posted'] = apartment.find('li', {'class': 'icon-time'})\
                        .text.strip()
                except:
                    _dict['Posted'] = None
                try:
                    link = apartment.find('a', {'class': 'result'}, href=True)
                    _dict['Link'] = link['href']
                except:
                    _dict['Link'] = None
                try:
                    img = apartment.find('img')
                    _dict['Photo'] = img['src']
                except:
                    _dict['Photo'] = None

                _list_page_content.append(_dict)

        logging.info('Uredno povučeni podatci s Index.hr oglasnika za izjanmljivanje stanova')
        return _list_page_content

    def get_df(self, list_page_content):
        df = pd.DataFrame(list_page_content)
        df = df.dropna()
        df = df.reset_index(drop=True)
        df.insert(loc=0, column='Id', value=df.index + 1)

        df['PriceEuro'] = df['PriceEuro'].str.replace('.', '').replace('0,00', 0).replace('', 0).astype('int64')
        df = df[df['PriceEuro'].between(100, 2000)]
        df['PriceKuna'] = df['PriceKuna'].str.replace('.', '').replace('0,00', 0).replace('', 0).astype('int64')
        df['Area'] = df['Area'].replace('', 0).astype('int64')
        df = df[df['Area'].between(10, 300)]
        df.insert(loc=6, column='CijenaKvadratEuro', value=df['PriceEuro']/df['Area'])
        df.insert(loc=7, column='CijenaKvadratKuna', value=df['PriceKuna'] / df['Area'])
        df['CijenaKvadratEuro'] = np.array(df['CijenaKvadratEuro'], np.int16)
        df['CijenaKvadratKuna'] = np.array(df['CijenaKvadratKuna'], np.int16)

        logging.info('Podatci s Index.hr oglasnika konvertirani u DataFrame')
        return df
