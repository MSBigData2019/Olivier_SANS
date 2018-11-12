# coding: utf-8
import re
import time
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool


request_headers = {
    'Accept-Language': 'en-US,en;q=0.5',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Connection': 'keep-alive'
}

def _build_bs4_from_url(url):
    res = requests.get(url)
    if res.status_code == 200:
        return BeautifulSoup(res.text, 'html.parser')
    print(res.status_code)
    return None

def get_argus_price(version, year):
    url_argus = 'https://www.lacentrale.fr/cote-auto-{}-{}-{}-{}.html'
    url = url_argus.format('renault', 'zoe', version.replace(' ', '+'), year)
    soup = _build_bs4_from_url(url)
    argus = soup.find('span', class_='jsRefinedQuot')
    if argus is not None:
        argus = argus.getText().strip().replace(' ', '')
    else:
        argus = np.NaN
    return argus


def _clean_data(df):
    reg = r'\D*(\d+)\D*(\d*)\D(\d*).*'
    df['KM'] = df['KM'].apply(lambda x: re.sub(reg, r'\1\2', str(x)))
    df['PRICE'] = df['PRICE'].apply(lambda x: re.sub(reg, r'\1\2', str(x)))
    df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
    df['KM'] = pd.to_numeric(df['KM'], errors='coerce')
    df['ARGUS'] = pd.to_numeric(df['ARGUS'], errors='coerce')
    return df

def scan_lacentrale(region):
    df = pd.DataFrame(columns=['VERSION', 'YEAR', 'KM', 'SELLER', 'PRICE', 'ARGUS'])
    url_prefix = 'https://www.lacentrale.fr/'
    url_scan = 'listing?makesModelsCommercialNames=RENAULT%3AZOE&regions={}&page={}'
    page = 1
    i = 1
    soup = _build_bs4_from_url(url_prefix+url_scan.format(region, page))
    total = int(soup.find('span', class_='numAnn').getText())
    while i < total:
        table = soup.find_all(class_='subContRight')
        if table:
            for car in table:
                version = car.find('span', class_='version').getText()
                year = car.find('div', class_='fieldYear').getText()
                km = car.find('div', class_='fieldMileage').getText()
                price = car.find('div', class_='fieldPrice').getText()
                seller = car.find('p', class_='typeSeller').getText()
                argus = get_argus_price(version.lower(), year)
                df.loc[i] = [version, year, km, seller, price, argus]
                i += 1
        page += 1
        soup = _build_bs4_from_url(url_prefix+url_scan.format(region, page))
    df = _clean_data(df)
    return df


def main():
    regions = ['FR-IDF', 'FR-PAC', 'FR-NAQ']
    df = pd.DataFrame(columns=['VERSION', 'YEAR', 'KM', 'SELLER' 'PRICE', 'ARGUS'])

    # start1 = time.time()
    # for region in regions:
    #     df = pd.concat([df, scan_lacentrale(region)],
    #                    axis=0, sort=False, ignore_index=True)
    # end1 = time.time()
    # print(f'Time without multiprocessing {end1-start1:.2f} s')

    start2 = time.time()
    p = Pool(3)
    df = pd.concat(p.map(scan_lacentrale, regions), axis=0, ignore_index=True)
    end2 = time.time()
    print(f'Time with multiprocessing {end2-start2:.2f} s')

    df['COTE'] = '+'
    df['COTE'].where(df['PRICE'] > df['ARGUS'], '-', inplace=True)
    print(df)
    df.to_json('centrale.json', orient='records', lines=True)
    # df.to_csv('centrale.csv', sep=';', index=False)


if __name__ == '__main__':
    main()
