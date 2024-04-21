import os
import time

import psycopg
from dotenv import load_dotenv

import crawl

load_dotenv()


def start():
    db_url = os.getenv("DATABASE_URL")
    connected = False
    while not connected:
        try:
            with psycopg.connect(db_url) as connection:
                connected = True
                create_tables(connection)
                crawl.start_crawling(connection)

        except Exception as e:
            print(e)
            time.sleep(0.5)
            pass


def create_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS "rawProduct" (
            id int8 PRIMARY KEY, 
            html TEXT);""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS links (
            id int8 PRIMARY KEY, 
            loc TEXT, 
            image TEXT, 
            crawled BOOLEAN);""")
        connection.commit()


start()
