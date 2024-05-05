import logging
import os
import time

import psycopg
from bs4 import BeautifulSoup
from dotenv import load_dotenv

skipped = 0


def increment_skipped():
    global skipped
    skipped += 1


def start_batched():
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
                concurrent_import_wine(connection)

        except Exception as e:
            logging.warning(e)
            time.sleep(0.5)
            logging.info("Connection to database lost. Retrying...")


def concurrent_import_wine(connection: psycopg.Connection):
    for i in range(0, 40000, 1000):
        import_many_wine(connection, i, 1000)


def import_many_wine(connection: psycopg.Connection, offset: int, limit: int):
    logging.info(f"Importing wine data from {offset} to {offset + limit}")
    products = get_many_raw_products(connection, offset, limit)
    if (len(products) == 0):
        logging.info("No more products to import.")
        return
    import_with_mega_queries(connection, products)


def import_with_mega_queries(connection: psycopg.Connection, products: [(dict, list[str])]):
    product_details_and_categories_list = []

    for product in products:
        try:
            product_details = (get_product_details(product[1], product[0]))
        except Exception as e:
            logging.error(f"{skipped} - Failed to get product details for product {product[0]}. {e}")
            increment_skipped()
            continue

        if product_details[0]["alcohol"] == 0:
            logging.info(f"{skipped} - Product {product_details[0]['name']} has no alcohol content. Skipping.")
            increment_skipped()
            continue

        product_details_and_categories_list.append(product_details)

    product_value_mapping = dict()
    insert_alkis_values = []

    all_categories = dict()
    insert_category_values = []

    product_category_mapping = dict()
    insert_alkis_category_values = []

    for idx, product in enumerate(product_details_and_categories_list):
        product_details = product[0]
        value_string_builder = []
        for key, value in product_details.items():
            product_value_mapping[f'{key}{idx}'] = value
            value_string_builder.append(f'%({key}{idx})s')
        insert_alkis_values.append(f'({", ".join(value_string_builder)})')

        product_category_mapping[f'product_id{idx}'] = product_details["product_id"]

        for category in product[1]:
            if category not in all_categories:
                all_categories[category] = category
                insert_category_values.append(f'(%({category})s)')

            if category not in product_category_mapping:
                product_category_mapping[category] = category

            insert_alkis_category_values.append(
                f'(%(product_id{idx})s, (SELECT category.id from category where category."categoryName" = %({category})s))')

    insert_alkis_query = f"""
                INSERT INTO "alkis" (id, name, price, "alcoholByVolume", volume, "pricePerAlcohol") 
                VALUES {", ".join(insert_alkis_values)} 
                ON CONFLICT (id) 
                DO UPDATE SET 
                    name = excluded.name, 
                    price = excluded.price, 
                    "alcoholByVolume" = excluded."alcoholByVolume",
                    volume = excluded.volume,
                    "pricePerAlcohol" = excluded."pricePerAlcohol";"""

    insert_category_query = f"""
                INSERT INTO category ("categoryName") 
                VALUES {", ".join(insert_category_values)} 
                ON CONFLICT ("categoryName") DO NOTHING;"""

    insert_alkis_category_query = f"""
                INSERT INTO "alkisCategory" ("alkisId", "categoryId") 
                VALUES {", ".join(insert_alkis_category_values)} 
                ON CONFLICT ("categoryId", "alkisId") DO NOTHING;"""

    with connection.cursor() as cursor:
        cursor.execute(insert_alkis_query, product_value_mapping)
        cursor.execute(insert_category_query, all_categories)
        cursor.execute(insert_alkis_category_query, product_category_mapping)
    connection.commit()


def get_many_raw_products(connection: psycopg.Connection, offset: int, limit: int) -> list[(dict, list[str])]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM \"rawProduct\" ORDER BY id OFFSET %(offset)s LIMIT %(limit)s;",
                       {"offset": offset, "limit": limit})
        products = cursor.fetchall()
    return products


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


start_batched()
