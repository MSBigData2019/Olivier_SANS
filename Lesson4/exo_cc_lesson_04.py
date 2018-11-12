# coding: utf-8
import re
import sys
import json
import requests
import pandas as pd

def load_data_para(url):
    res = requests.get(url)
    res_json = json.loads(res.text)
    return pd.read_json(res.text)


def read_info(code):
    url = f'https://www.open-medicaments.fr/api/v1/medicaments/{code}'
    res = requests.get(url).json()
    L = []
    res_substance = res['compositions'][0]['substancesActives'][0]
    L.append(res_substance['denominationSubstance'])
    L.append(res['titulaires'][0])
    L.append(res_substance['dosageSubstance'].split()[0].replace(',', '.'))
    L.append(res_substance['dosageSubstance'].split()[1])
    L.append(res['formePharmaceutique'])
    L.append(res['etatCommercialisation'])
    L.append(res['presentations'][0]['prix'])
    return L[0], L[1], L[2], L[3], L[4], L[5], L[6]

def get_info(df):
    L = ['Nom', 'Labo', 'dosage', 'unite', 'forme', 'commercialisation', 'prix']
    df[L[0]], df[L[1]], df[L[2]], df[L[3]], df[L[4]], df[L[5]], df[L[6]] = zip(*df['codeCIS'].apply(lambda x: read_info(str(x))))
    return df

def dosage_mg(df):
    df['mul'] = 1000
    df['mul'] = df['mul'].where(df['unite'] == 'g', 1)
    df['dosage'] = df['dosage'].fillna(0).astype(float)*df['mul']
    df.rename(columns={'dosage': 'dosage(mg)'}, inplace=True)
    df.drop(['mul', 'unite'], axis=1, inplace=True)
    return df

def by_using_the_full_api(url):
    df = load_data_para(url)
    del df['denomination']
    df = get_info(df)
    df_clean = dosage_mg(df)
    return df_clean

def by_regex(url):
    df = load_data_para(url)
    reg = r'([\w]*)\s([\D]*)\s(\d+)\s(.*),(.*)'
    denom = df['denomination']
    df_temp = denom.str.extract(reg)
    df_temp.rename(columns={0:'Nom', 1:'Labo', 2:'dosage',
                            3:'unite', 4:'forme'}, inplace=True)
    df_temp = dosage_mg(df_temp)
    df_clean = pd.concat([df['codeCIS'], df_temp], axis=1)
    return df_clean

def main():
    url = 'https://open-medicaments.fr/api/v1/medicaments?query=paracetamol'
    print(f"Using regex from {url}")
    df1 = by_regex(url)
    print(df1)
    print(f"Search more information with the api")
    df2 = by_using_the_full_api(url)
    print(df2)

if __name__ == '__main__':
    main()
