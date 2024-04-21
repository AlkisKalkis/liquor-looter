from bs4 import BeautifulSoup


def beautify(html):
    soup = BeautifulSoup(html, 'html.parser')
    product_details = soup.find('div', {'class': 'product__details'})
    for script in product_details.find_all('script'):
        script.extract()
    for expandable in product_details.find_all('div', {'class': 'expandable'}):
        expandable.extract()
    for noPrint in product_details.select('.no-print'):
        noPrint.extract()
    for button in product_details.select('button'):
        button.extract()
    product_details.find('div', {'class': 'product__image-container'}).extract()
    return str(product_details)
