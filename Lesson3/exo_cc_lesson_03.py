# coding: utf-8
import re
import json
import requests
import pandas as pd

def load_data_villes(url):
    return pd.read_html(url, encoding='utf-8')[0]


def _clean_columns_commune_and_dept(df):
    df['Commune'] = df['Commune'].apply(lambda x: str(x).split('[')[0])
    df['Département'] = df['Département'].apply(lambda x: str(x).split('[')[0])
    return df


def _clean_and_parse_columns_population_by_years(df):
    rep = re.compile(r'\D*(\d+)\D*(\d+)\D*(\d*),.*')
    df = df.applymap(lambda x: re.sub(rep, r'\1\2\3', str(x)))
    df['2015'] = df['2015'].astype('int64')
    df['2011'] = df['2011'].astype('int64')
    return df

def _patch_problem_of_columns_names(df):
    df.columns = df.iloc[0].copy(False)
    df = df.reindex(df.index.drop(0))
    df.columns = df.columns.str.split('[').str[0].values
    return df

def clean_data(df):
    df = _patch_problem_of_columns_names(df)
    df = _clean_columns_commune_and_dept(df)
    df_clean = _clean_and_parse_columns_population_by_years(df)
    df_clean = df_clean[['Commune', 'Département', 'Région', '2015', '2011']]
    return df_clean

def sort_top_x_according_year(df, top, year):
    table_top_x = df.sort_values(by=[year], ascending=False)[0:top]
    return table_top_x

def _get_api_key():
    key = open('api_keys.txt', 'r')
    return key.read().split('\n')[1]

def _call_google_api(communes):
    api_url = 'https://maps.googleapis.com/maps/api/distancematrix/json?units=metrics'
    key = _get_api_key()
    api_call = f'{api_url}&origins={communes}&destinations={communes}&key={key}'
    return requests.get(api_call)

def _parse_result_of_api(res):
    json_res = json.loads(res.content)
    if json_res['status'] == 'REQUEST_DENIED':
        return json_res['error_message']
    return list(map(lambda x: list(map(lambda y: y['distance']['text'],
                                       x['elements'])), json_res['rows']))

def get_distances(df):
    communes = '|'.join(df['Commune'])
    res = _call_google_api(communes)
    return _parse_result_of_api(res)


def main():
    url_wiki = "https://fr.wikipedia.org/wiki/Liste_des_communes_de_France_les_plus_peupl%C3%A9es"
    df_villes = load_data_villes(url_wiki)
    df_villes_clean = clean_data(df_villes)
    # En 2015, MONTPELLIER passe devant STRASBOURG
    df_sorted = sort_top_x_according_year(df_villes_clean, 10, '2015')
    response = get_distances(df_sorted)
    print(response)
    # table.apply(lambda x: pd.api.types.infer_dtype(x.values))

if __name__ == '__main__':
    main()
