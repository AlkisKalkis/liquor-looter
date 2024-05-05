import logging
import os
import time

import psycopg
from bs4 import BeautifulSoup
from dotenv import load_dotenv


def start():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    connected = False
    logging.basicConfig(level=logging.INFO)
    logging.info("Connecting to database")
    while not connected:
        try:
            with psycopg.connect(db_url) as connection:
                connected = True
                logging.info("Connected to database successfully")
                import_wine(connection)

        except Exception as e:
            logging.warning(e)
            time.sleep(0.5)
        logging.info("Connection to database lost. Retrying...")


def import_wine(connection: psycopg.Connection):
    logging.info("Importing wine data")
    # Implement the wine importer here
    index = 0
    product = get_next_raw_product(connection, index)
    while product and index < 2500:
        index += 1
        logging.info(f"Processing index {index}.")
        try:
            product_details, categories = get_product_details(product[1], product[0])
        except Exception as e:
            logging.error(f"Failed to get product details for product {product[0]}. {e}")
            product = get_next_raw_product(connection, index)
            continue

        if product_details["alcohol"] == 0:
            logging.info(f"Product {product_details['name']} has no alcohol content. Skipping.")
            product = get_next_raw_product(connection, index)
            continue

        logging.info(f"Importing product {product_details['name']}")

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO "alkis" (id, name, price, "alcoholByVolume", volume, "pricePerAlcohol") 
                VALUES (%(product_id)s, %(name)s, %(price)s, %(alcohol)s, %(volume)s, %(pricePerAlcohol)s) 
                ON CONFLICT (id) 
                DO UPDATE SET 
                    name = %(name)s, 
                    price = %(price)s, 
                    "alcoholByVolume" = %(alcohol)s, 
                    volume = %(volume)s,
                    "pricePerAlcohol" = %(pricePerAlcohol)s;""",
                           product_details)

            for category in categories:
                cursor.execute("""
                        INSERT INTO category ("categoryName") 
                        VALUES (%(category)s) 
                        ON CONFLICT ("categoryName") DO NOTHING;""",
                               {"category": category})

                cursor.execute("""
                        INSERT INTO "alkisCategory" ("alkisId", "categoryId") 
                        VALUES (
                            %(product_id)s, 
                            (SELECT category.id from category where category."categoryName" = %(category)s)) 
                        ON CONFLICT ("categoryId", "alkisId") DO NOTHING;""",
                               {"product_id": product_details["product_id"], "category": category})
        connection.commit()
        product = get_next_raw_product(connection, index)


def get_next_raw_product(connection: psycopg.Connection, index: int) -> (dict, list[str]):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM \"rawProduct\" OFFSET %(index)s LIMIT 1;", {"index": index})
        product = cursor.fetchone()
    return product


def get_product_details(html: str, product_id: int) -> tuple[dict[str, str | int], list[str]]:
    soup = BeautifulSoup(html, 'html.parser')

    price_string = "".join(soup.find('span', {'class': 'product__price'}).text.split(' ')[1:])
    price = int(price_string.replace(',', ''))

    name = soup.find('h1', {'class': 'product__name'}).text

    alcohol_string = soup.find('strong', string='Alkohol').find_next('span').text.strip('%')
    alcohol = int(float(alcohol_string.replace(',', '.')) * 10)

    volume_string = soup.find('span', {'class': 'amount'}).text.split(' ')[0]
    volume = int(float(volume_string.replace(',', '.')) * 10)

    categories = soup.find('p', {'class': 'product__category-name'}).text.split(' - ')

    price_per_alcohol = 0
    if alcohol != 0:
        price_per_alcohol = price / ((volume / 1000) * (alcohol / 1000))

    return {"product_id": product_id, "name": name, "price": price, "alcohol": alcohol, "volume": volume,
            "price_per_alcohol": price_per_alcohol}, categories


start()
