# coding: utf-8
import sys
import unittest
import requests
import pandas as pd
import numpy as np



def get_mean(url, api_token):
    headers = {'Authorization': 'token {}'.format(api_token)}
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        data = pd.read_json(res.text)
        if "stargazers_count" in data.columns:
            return np.mean(data["stargazers_count"])
    return 0

def get_mean_stars(df_users, api_token):
    url_template = "https://api.github.com/users/{}/repos?page={}&per_page=30"
    df_users["url"] = df_users["User"].apply(lambda x: url_template.format(x, 1))
    df_users["Mean of Stars"] = df_users["url"].apply(lambda x: get_mean(x, api_token))
    return df_users


def put_users_to_df(url):
    users_page = requests.get(url)
    if users_page.status_code == 200:
        document = pd.read_html(users_page.text)[0]
        document["User"] = document["User"].str.split().str[0]
        return document[["User", "Location"]]

    print("erreur code :", users_page.status_codes)
    exit(0)

def main(access_token):
    ## Call to unittest ##
    #unittest.main()
    ######################
    url_git_repo = "https://gist.github.com/paulmillr/2657075"
    df_users = put_users_to_df(url_git_repo)
    df_users = get_mean_stars(df_users, access_token)
    df_users = df_users[["User", "Location", "Mean of Stars"]]
    df_users = df_users.sort_values(by=["Mean of Stars"], ascending=False)
    df_users = df_users.reset_index(drop=True)
    df_users.to_csv("data_git_top_contributor.csv", sep=";")


if __name__ == '__main__':
    main(sys.argv[1])
