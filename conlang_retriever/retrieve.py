import requests, bs4, io, sqlite3
import pandas as pd

URL = 'https://database.conlang.org/search/'

def make_df(url: str=URL) -> pd.DataFrame:
    '''
    Make a pandas dataframe from a webpage with an HTML table.
    Arguments:
        Optional:
        `url: str=URL`: the url to request from, conlang database by default.
    Returns:
        `pd.DataFrame`: the dataframe made from the table.
    '''
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    df = pd.read_html(io.StringIO(u'%s' %str(table)))[0].drop('Unnamed: 0', axis=1)
    return df.drop('Unnamed: 0', axis=1) if 'Unnamed: 0' in df.columns else df

def convert_row(row: tuple[int, pd.Series]) -> str:
    '''
    Convert row in a DataFrame to a string for insertion in a SQLite database.
    Arguments:
        `row: tuple[int, pd.Series]`: an item in the generator produced by the `.iterrows()` method on a DataFrame.
    Returns:
        `str`: the row to be inserted.
    '''
    return str(tuple(row[1].fillna('').values)).replace("\\'", ' ')

def convert_db(df: pd.DataFrame) -> None:
    '''
    Convert pandas DataFrame to SQLite database and write to file.
    Arguments:
        `df: pd.DataFrame`: the DataFrame to convert.
    Returns:
        `None`
    '''
    connection = sqlite3.connect('data/conlangs.db')
    cursor = connection.cursor()
    names = ", ".join(name.lower().replace(' ', '_') for name in df.columns.values)
    cursor.execute(f'CREATE TABLE conlangs({names})')
    values = ", ".join(convert_row(row) for row in df.iterrows())
    cursor.execute(f'INSERT INTO conlangs VALUES {values}')
    connection.commit()