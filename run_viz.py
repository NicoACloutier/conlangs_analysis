from conlang_retriever import retrieve
import dash, pandas as pd

def main():
    retrieve.convert_db(retrieve.make_df(retrieve.URL))
    app = dash.Dash(__name__)
    app.layout = dash.html.Div([])
    app.run(debug=True)

if __name__ == '__main__':
    main()