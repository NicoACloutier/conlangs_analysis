from conlang_retriever import retrieve
import dash, plotly, sqlite3, pyspark, pandas as pd

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
    data = cursor.execute('SELECT * FROM conlangs;').fetchall()
    columns = [column[1] for column in cursor.execute('PRAGMA table_info(conlangs);').fetchall()]
    return spark.createDataFrame(data, schema=columns)

def main():
    retrieve.convert_db(retrieve.make_df(retrieve.URL)) # retrieve data and make database if necessary
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    data = read_database(spark)
    
    app = dash.Dash(__name__)
    app.layout = dash.html.Div([])
    app.run(debug=True)

if __name__ == '__main__':
    main()