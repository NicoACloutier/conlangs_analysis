from conlang_retriever import retrieve

def main():
    retrieve.convert_db(retrieve.make_df(retrieve.URL))

if __name__ == '__main__':
    main()