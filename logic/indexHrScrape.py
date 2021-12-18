from bs4 import BeautifulSoup
import requests
import pandas as pd
import logging


class IndexHrScrape:

    def get_page_content(self, pages):
        _list_page_content = []
        for page in range(1, pages):
            url = f'https://www.index.hr/oglasi/najam-stanova/gid/3279?pojamZup=-2&tipoglasa=1&sortby=1&elementsNum=' \
                  f'100&grad=0&naselje=0&cijenaod=0&cijenado=370000&vezani_na=988-887_562-563_978-1334&num={page}'
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
        df['PriceKuna'] = df['PriceKuna'].str.replace('.', '').replace('0,00', 0).replace('', 0).astype('int64')
        df['Area'] = df['Area'].replace('', 0).astype('int64')
        df.insert(loc=6, column='CijenaKvadratEuro', value=round((df['PriceEuro']/df['Area']), 2))
        df.insert(loc=7, column='CijenaKvadratKuna', value=round((df['PriceKuna'] / df['Area']), 2))

        logging.info('Podatci s Index.hr oglasnika konvertirani u dataFrame')
        return df
