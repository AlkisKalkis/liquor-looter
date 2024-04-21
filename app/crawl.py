import logging
import random
import time

import psycopg
import requests
from bs4 import BeautifulSoup

from beautiful import beautify
from fetch import fetch_product


def start_crawling(connection: psycopg.Connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM links WHERE crawled = FALSE;")
        count = cursor.fetchone()[0]
    if count == 0:
        logging.info("No links to crawl. Fetching new links.")
        fetch_links(connection)

    crawl_links(connection)


def crawl_links(connection: psycopg.Connection):
    link = get_next_link(connection)
    while link:
        logging.info(f"Crawling link {link[1]}")
        crawl_link(connection, link[0], link[1])
        link = get_next_link(connection)
        time.sleep(8 + random.random() * 3)

    logging.info("Crawled all links. Shutting down.")


def get_next_link(connection: psycopg.Connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM links WHERE crawled = FALSE LIMIT 1;")
        link = cursor.fetchone()
    return link


def crawl_link(connection: psycopg.Connection, product_id: int, loc: str):
    html = fetch_product(loc)
    beautiful_html = beautify(html)
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO "rawProduct" (id, html) 
            VALUES (%(product_id)s, %(html)s) 
            ON CONFLICT (id) DO UPDATE SET html = %(html)s;""",
                       {"product_id": product_id, "html": beautiful_html})
        cursor.execute("""
            UPDATE links SET crawled = TRUE WHERE id = %(product_id)s;""", {"product_id": product_id})

    connection.commit()


def fetch_links(connection: psycopg.Connection):
    sitemap = requests.get("https://vinmonopolet.no/sitemap.xml")
    soup = BeautifulSoup(sitemap.content, 'xml')

    # Find all loc tags with product in the url
    locs = [loc.text for loc in soup.find_all('loc') if 'Product-no-NOK' in loc.text]

    product_params = []
    for loc in locs:
        products = requests.get(loc)
        soup = BeautifulSoup(products.content, 'xml')
        for url in soup.find_all('url'):
            loc = url.find('loc').text
            image = url.find('image:loc').text
            product_params.append({'loc': loc, 'image': image, 'crawled': False, 'id': int(loc.split('/')[-1])})

    with connection.cursor() as cursor:
        # Insert all links into the database
        cursor.executemany("""
            INSERT INTO links (id, loc, image, crawled)
            VALUES (%(id)s, %(loc)s, %(image)s, %(crawled)s)
            ON CONFLICT (id) DO UPDATE SET loc = %(loc)s, image = %(image)s, crawled = %(crawled)s;""",
                           product_params)
    connection.commit()
