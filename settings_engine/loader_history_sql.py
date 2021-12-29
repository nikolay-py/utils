import csv
import os
from typing import List

import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')

conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, 
    database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)


file_memory = None
basedir = os.path.abspath(os.path.dirname(__file__))
table_list = ['rs24', 'lerua', 'maxipro', 'petrovich', 'elcom']


def save_dataset(cursor, table: str, csv_file_object) -> None:
    for row in csv_file_object:
        cursor.execute(
            f"INSERT INTO {table} (id,title,category,price,discount_price,link,site,catalog,stock,stock_name,article,"
            "unit,is_order,inventory,city,price_2,discount_price_2,unit_2,date) VALUES (default , %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            row
        )


def create_table(cursor, table):
    cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table}(
            id SERIAL PRIMARY KEY,
            title text,
            category text,
            price numeric,
            discount_price numeric,
            link text,
            site text,
            catalog text,
            stock int,
            stock_name text,
            article text,
            unit text,
            is_order int,
            inventory text,
            city text,
            price_2 text,
            discount_price_2 text,
            unit_2 text,
            date date 
        )
        """)

def session_connect(table: str, csv_file_object, move_from: str, move_to: str) -> None:
    try:
        with conn:
            with conn.cursor() as cursor:
                create_table(cursor, table)
        with conn:
            with conn.cursor() as cursor:
                save_dataset(cursor, table, csv_file_object)

        os.rename(move_from, move_to)
        print(f'Файл сохранен в базу {move_from}')

    except (Exception, psycopg2.DatabaseError) as error:
        print(f'{error}\n\nОшибка в фале {move_from}')


def create_list_files(start_name: str) -> List[str]:
    newlist = []
    for names in os.listdir("./for_loading/."):
        if names.endswith(".csv") and names.startswith (start_name):
            newlist.append(names)
    return newlist


def read_file(csv_file: str):
    global file_memory
    file_memory = open(csv_file, 'r', encoding="UTF-8")
    reader = csv.reader(file_memory)
    next(reader)  # Убрать заголовок

    return reader


def save_in_db(table: str, csv_file: str) -> None:
    move_from = os.path.join(basedir,'for_loading', csv_file)
    move_to = os.path.join(basedir, 'archive', csv_file)
    csv_file_object = read_file(move_from)
    session_connect(table, csv_file_object, move_from, move_to)
    file_memory.close()


def inventory_files(table_list: List[str]) -> None:
    for table in table_list:
        start_name = table[:2]
        list_files = create_list_files(start_name)
        for csv_file in list_files:
            save_in_db(table, csv_file)


inventory_files(table_list)
conn.close()
