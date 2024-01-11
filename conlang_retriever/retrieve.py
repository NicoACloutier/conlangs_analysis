import requests, bs4, io
import pandas as pd

URL = 'https://database.conlang.org/search/'

def make_df(url:str=URL) -> pd.DataFrame:
    '''
    Make a pandas dataframe from a webpage with an HTML table.
    Arguments:
        Optional:
        `url:str=URL`: the url to request from, conlang database by default.
    Returns:
        `pd.DataFrame`: the dataframe made from the table.
    '''
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    df = pd.read_html(str(table))[0].drop('Unnamed: 0', axis=1)
    return df.drop('Unnamed: 0', axis=1) if 'Unnamed: 0' in df.columns else df