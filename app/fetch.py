import logging

import requests


def fetch_product(url):
    try:
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36'

        session = requests.Session()
        session.headers['User-Agent'] = user_agent
        response = session.get(url, allow_redirects=True)
        return response.content.decode('utf-8')
    except Exception as e:
        logging.error("Failed to fetch product with URL: " + url + " " + str(e))
        pass
