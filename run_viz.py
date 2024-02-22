from conlang_retriever import retrieve
import plotly.express as px
import dash_bootstrap_components as dbc
import dash_bootstrap_templates as dbt
import dash, collections, plotly, sqlite3, pyspark, findspark, pandas as pd

MINIMUM_YEAR = 1000 # the minimum year for included conlangs in the time histogram
YEAR_COLUMN = 'start_year' # the column of the database with year information
TYPE_COLUMN = 'type' # the column of the database with type information
HIST_BINS = 20 # number of bins for the histogram

def read_database(spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    '''
    Read local SQLite database and convert to PySpark `DataFrame`.
    Arguments:
        `spark: pyspark.sql.SparkSession`: the PySpark session to be utilized for `DataFrame` hosting.
    Returns:
        `pyspark.sql.DataFrame`: the PySpark `DataFrame` object.
    '''
    connection = sqlite3.connect(retrieve.DB_LOCATION)
    cursor = connection.cursor()
    data = cursor.execute('SELECT * FROM conlangs;').fetchall() # select all data from the `conlangs` table in the scraped database
    columns = [column[1] for column in cursor.execute('PRAGMA table_info(conlangs);').fetchall()] # find all column names for the `conlangs` table
    return spark.createDataFrame(data, schema=columns)

def make_time_histogram(df: pyspark.sql.DataFrame, column: str=YEAR_COLUMN, minimum_year: int=MINIMUM_YEAR) -> plotly.graph_objects.Figure:
    '''
    Make a timeline histogram from the time data.
    Arguments:
        `df: pyspark.sql.DataFrame`: the PySpark dataframe utilized to create the histogram.
        Optional:
        `column: str=YEAR_COLUMN`: the name of the column containing start years for conlangs.
        `minimum_year: int=MINIMUM_YEAR`: the minimum year to be included in the histogram.
    Returns:
        `plotly.graph_objects.Figure`: a plotly `Figure` object to be included in the dashboard.
    '''
    df = df.filter(df[column] != '')
    int_udf = pyspark.sql.functions.udf(int, pyspark.sql.types.IntegerType())   
    df.withColumn(column, int_udf(pyspark.sql.functions.col(column)))
    df = df.filter(2500 > df[column])
    df = df.filter(df[column] > minimum_year)
    years = df.select(column).collect()
    years = sorted([int(float(row.start_year)) for row in years])
    return px.histogram(years, nbins=HIST_BINS, template="darkly")

def split_types(type_string: str) -> list[str]:
    '''
    Split type string into type values.
    Arguments:
        `type_string: str`: the type string to split.
    Returns:
        `list[str]`: the split string.
    '''
    split_result = type_string.split(', ')
    original = split_result[:-1]
    if split_result[-1].startswith('and '):
        final = [split_result[-1][4:],]
    elif ' and ' in split_result[-1]:
        final = split_result[-1].split(' and ')
    else:
        final = [split_result[-1],]
    return original + final

def make_type_bar(df: pyspark.sql.DataFrame, column: str=TYPE_COLUMN) -> plotly.graph_objects.Figure:
    '''
    Make a bar graph of the given types in the type data.
    Arguments:
        `df: pyspark.sql.DataFrame`: the PySpark dataframe utilized to create the histogram.
        Optional:
        `column: str=TYPE_COLUMN`: the name of the column containing type information for conlangs.
    Returns:
        `plotly.graph_objects.Figure`: a plotly `Figure` object to be included in the dashboard.
    '''
    df = df.filter(df[column] != '')
    # split_udf = pyspark.sql.functions.udf(split_types, pyspark.sql.types.IntegerType())
    # df.withColumn(column, split_udf(pyspark.sql.functions.col(column)))
    types = df.select(column).collect()
    types = sum([split_types(row.type) for row in types], [])
    type_counter = collections.Counter(types)
    type_dict = {'types': list(type_counter.keys()), 'vals': list(type_counter.values())}
    return px.bar(type_dict, x='types', y='vals')

def main():
    findspark.init()
    retrieve.convert_db(retrieve.make_df(retrieve.URL)) # retrieve data and make database if necessary
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    data = read_database(spark)

    dbt.load_figure_template('darkly')
    
    # create visualizations
    time_histogram = make_time_histogram(data)
    type_bar = make_type_bar(data)
    style = {'color': '#fa677f',
             'text-indent': '30px',
             'font': '100% system-ui'}
    
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
    app.layout = dash.html.Div(style=style, children=[
        dbc.Row(dbc.Col(dash.html.H1(children='Conlangs database analysis'))),

        dbc.Row(dbc.Col(dash.html.Div(children='''
            An analysis of the online and freely available conlangs database.
        '''))),

        dbc.Row(dbc.Col(dash.dcc.Graph(
            id='time_histogram',
            figure=time_histogram
        ))),
        
        dbc.Row(dbc.Col(dash.dcc.Graph(
            id='type_bar',
            figure=type_bar
        )))
    ])
    app.run(debug=True)

if __name__ == '__main__':
    main()