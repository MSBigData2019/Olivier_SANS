# coding: utf-8
import unittest
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd


def search_company_on_reuters(company):
    url_research = "http://www.reuters.com/finance/stocks/lookup"
    url_finance_page = "https://www.reuters.com/finance/stocks/financial-highlights/"
    params_research = {"searchType":"any", "comSortBy":"marketcap",\
                       "sortBy":"", "dateRange":"", "search":str(company)}
    page_research = requests.get(url=url_research, params=params_research)
    if page_research.status_code == 200:
        soup_research = BeautifulSoup(page_research.text, "html.parser")
        all_companies = soup_research.find("div", class_="search-companies-count")
        if all_companies is None:
            print("no match for :", company)
            return None, None
        first_company_id = all_companies.parent.find_next("td")\
                                               .find_next_sibling().string
        return requests.get(url_finance_page+str(first_company_id)), first_company_id
    print("problem of connexion during the research of :", company)
    return None, None


def _clean_string_and_parse_to_float(string):
    try:
        return float(string.strip("\t\n()% ").replace(",", ""))
    except ValueError:
        return "NC"


def _get_stock_exchange_and_percent_change_information(soup):
    stock_exchange = soup.find("div", class_="sectionQuote nasdaqChange")\
                        .find("span", class_="nasdaqChangeHeader")\
                        .find_next("span").string
    stock_exchange_parsed = _clean_string_and_parse_to_float(stock_exchange)
    percent_change = soup.find("div", class_="sectionQuote priceChange")\
                         .find("span", class_="valueContentPercent").text
    percent_change_parsed = _clean_string_and_parse_to_float(percent_change)
    return [stock_exchange_parsed, percent_change_parsed]


def _get_institutional_holders_share_owned(soup):
    share_owned = soup.find("div", class_="column2 gridPanel grid4")\
                      .find("td", text=re.compile(".Shares Owned."))\
                      .find_next_sibling().text
    share_owned_parsed = _clean_string_and_parse_to_float(share_owned)
    return share_owned_parsed


def _get_quarter_dec_18_information(soup):
    quarter_dec_18_tab = soup.find("div", class_="column1 gridPanel grid8")\
                             .find("td", text=re.compile(".*SALES.*"))\
                             .parent.find_next_sibling().select("td")
    quarter_dec_18_mean = _clean_string_and_parse_to_float(quarter_dec_18_tab[2].string)
    quarter_dec_18_high = _clean_string_and_parse_to_float(quarter_dec_18_tab[3].string)
    quarter_dec_18_low = _clean_string_and_parse_to_float(quarter_dec_18_tab[4].string)
    return [quarter_dec_18_mean, quarter_dec_18_high, quarter_dec_18_low]


def _get_divident_yield_information(soup):
    divident_yield_tab = soup.find("div", class_="column1 gridPanel grid8")\
                             .find("td", text=re.compile(".*Dividend Yield.*"))\
                             .parent.select("td")
    company_divident_yield = _clean_string_and_parse_to_float(divident_yield_tab[1].string)
    industry_divident_yield = _clean_string_and_parse_to_float(divident_yield_tab[2].string)
    sector_divident_yield = _clean_string_and_parse_to_float(divident_yield_tab[3].string)
    return [company_divident_yield, industry_divident_yield, sector_divident_yield]


def get_financial_information_on_reuters(soup):
    financial_info = []
    financial_info.extend(_get_stock_exchange_and_percent_change_information(soup))
    financial_info.append(_get_institutional_holders_share_owned(soup))
    financial_info.extend(_get_divident_yield_information(soup))
    financial_info.extend(_get_quarter_dec_18_information(soup))
    return financial_info


def build_dataframe(all_data):
    pd.set_option("display.max_columns", None)
    columns_data = ["Company Name", "Stock Exchange (EUR)", "Percent Change (%)",
                    "Shares Owned (%)", "Company Divident Yield",
                    "Industry Divident Yield", "Sector Divident Yield",
                    "Quarter Dec-18 Mean (M)", "Quarter Dec-18 High (M)",
                    "Quarter Dec-18 Low (M)"]
    return pd.DataFrame(data=all_data, columns=columns_data)

def _to_xml(dframe):
    res = "<item>\n"
    def row_to_xml(row):
        xml = ["\t<item>"]
        for i, col_name in enumerate(row.index):
            xml.append("\t\t<field name=\"{0}\">{1}</field>".format(col_name, row.iloc[i]))
        xml.append("\t</item>")
        return "\n".join(xml)
    res = "\n".join(dframe.apply(row_to_xml, axis=1))
    return res

def write_xml_file(financial_info_dataframe, filename, mode="w"):
    res = _to_xml(financial_info_dataframe)
    res = "<item>\n"+res+"\n</item>"
    with open(filename, mode) as fwr:
        fwr.write(res)

def load_xml_file_from_local(location):
    with open(location) as fread:
        soup_xml = BeautifulSoup(fread, "xml")
    return soup_xml


class CralingUnittest(unittest.TestCase):
    def tests_clean_string_and_parse_to_float(self):
        self.assertEqual(_clean_string_and_parse_to_float(""), "NC")
        self.assertEqual(_clean_string_and_parse_to_float("\t\n \t1\n3\t2 "), "NC")


def main():

    ## Call to unittest ##
    #unittest.main()
    ######################


    companies = ("airbus", "lvmh", "danone")
    all_data = []
    num_row = 0
    for company in companies:
        res, company_name = search_company_on_reuters(company)
        if res is not None and res.status_code == 200:
            all_data.append([])
            html_doc = res.text
            soup = BeautifulSoup(html_doc, "html.parser")
            all_data[num_row].append(company_name)
            all_data[num_row].extend(get_financial_information_on_reuters(soup))
            num_row += 1

    if not all_data:
        print("no company found")
        exit(0)

    financial_info_dataframe = build_dataframe(all_data)
    print(financial_info_dataframe)


    ## Transform the dataframe -> xml and xml -> BeautifulSoup ##
    ##          Note : Both work but comment for exam          ##
    #############################################################

    # write_xml_file(financial_info_dataframe, "./fichier_finance.xml")
    # soup_xml = load_xml_file_from_local("./fichier_finance.xml")
    # print(soup_xml.find("field", text=re.compile(".*PA"))\
    #       .find_next_sibling().string)

    ## Transform the dataframe -> json file and json file -> dataframe ##
    ##              Note : Both work but comment for exam              ##
    #####################################################################

    # financial_info_dataframe.to_json("fichier_finance.json")
    # json_loc = "./fichier_finance.json"
    # duplicat = pd.read_json(json_loc)
    # print(duplicat)
    # print(duplicat.dtypes)

if __name__ == '__main__':
    main()
