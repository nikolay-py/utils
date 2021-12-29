import os
from typing import List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import TypeEngine

load_dotenv()

DB_NAME = os.environ.get('DB_URL')
engine = create_engine(DB_NAME)
basedir = os.path.abspath(os.path.dirname(__file__))
table_list = ['rs24', 'lerua', 'maxipro', 'petrovich', 'elcom']


def create_list_files(start_name: str) -> List[str]:
    newlist = []
    for names in os.listdir("./for_loading/."):
        if names.endswith(".csv") and names.startswith (start_name):
            newlist.append(names)
    return newlist


def save_in_db(conn: TypeEngine, table:str, csv_file: str) -> None:
    
    move_from = os.path.join(basedir,'for_loading', csv_file)
    move_to = os.path.join(basedir, 'archive', csv_file)

    try: 
        df = pd.read_csv(move_from)
        df = df.rename(columns=({'order': 'is_order'}))
        df.to_sql(table, con=conn, if_exists='append', index=False)

        os.rename(move_from, move_to)

    except Exception as error:
        print(f'{error}\nОшибка в файле {csv_file}')


def session_connect(table_list: List[str]) -> None:
    with engine.connect().execution_options(autocommit=True) as conn:
        for table in table_list:

            start_name = table[:2]
            list_files = create_list_files(start_name)
            
            for csv_file in list_files:
                save_in_db(conn, table, csv_file)


session_connect(table_list)
