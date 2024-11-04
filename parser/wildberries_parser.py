import requests
import os
import psycopg2
from dotenv import load_dotenv
from retry import retry
from datetime import datetime

# Загружаем конфигурацию окружения
load_dotenv()

# Подключение к БД
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

def get_catalogs_wb() -> dict:
    """Получаем полный каталог Wildberries"""
    url = 'https://static-basket-01.wbbasket.ru/vol0/data/main-menu-ru-ru-v3.json'
    headers = {'Accept': '*/*', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    return requests.get(url, headers=headers).json()

def get_data_category(catalogs_wb: dict) -> list:
    """Сбор данных категорий из каталога Wildberries"""
    catalog_data = []
    if isinstance(catalogs_wb, dict) and 'childs' not in catalogs_wb:
        catalog_data.append({
            'name': f"{catalogs_wb['name']}",
            'shard': catalogs_wb.get('shard', None),
            'url': catalogs_wb['url'],
            'query': catalogs_wb.get('query', None)
        })
    elif isinstance(catalogs_wb, dict):
        catalog_data.append({
            'name': f"{catalogs_wb['name']}",
            'shard': catalogs_wb.get('shard', None),
            'url': catalogs_wb['url'],
            'query': catalogs_wb.get('query', None)
        })
        catalog_data.extend(get_data_category(catalogs_wb['childs']))
    else:
        for child in catalogs_wb:
            catalog_data.extend(get_data_category(child))
    return catalog_data

def search_category_in_catalog(url: str, catalog_list: list) -> dict:
    """Поиск категории по ссылке"""
    for catalog in catalog_list:
        if catalog['url'] == url.split('https://www.wildberries.ru')[-1]:
            print(f'Категория найдена: {catalog["name"]}')
            return catalog

@retry(Exception, tries=5, delay=1)
def scrap_page(page: int, shard: str, query: str, low_price: int, top_price: int, discount: int = None) -> dict:
    """Сбор данных со страниц"""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0)"}
    url = f'https://catalog.wb.ru/catalog/{shard}/catalog?appType=1&curr=rub' \
          f'&dest=-1257786' \
          f'&locale=ru' \
          f'&page={page}' \
          f'&priceU={low_price * 100};{top_price * 100}' \
          f'&sort=popular&spp=0' \
          f'&{query}' \
          f'&discount={discount}'
    response = requests.get(url, headers=headers)
    return response.json()

def get_data_from_json(json_file: dict) -> list:
    """Извлекаем данные о товарах из JSON"""
    data_list = []
    for data in json_file['data']['products']:
        data_list.append({
            'id': data.get('id'),
            'name': data.get('name'),
            'price': int(data.get("priceU") / 100),
            'salePriceU': int(data.get('salePriceU') / 100),
            'cashback': data.get('feedbackPoints'),
            'sale': data.get('sale'),
            'brand': data.get('brand'),
            'rating': data.get('rating'),
            'supplier': data.get('supplier'),
            'supplierRating': data.get('supplierRating'),
            'feedbacks': data.get('feedbacks'),
            'reviewRating': data.get('reviewRating'),
            'promoTextCard': data.get('promoTextCard'),
            'promoTextCat': data.get('promoTextCat'),
            'link': f'https://www.wildberries.ru/catalog/{data.get("id")}/detail.aspx?targetUrl=BP'
        })
    return data_list

def save_to_db(data_list):
    """Сохраняем данные в БД с учетом нормализованной структуры"""
    for product in data_list:
        # Вставляем бренд, если его нет
        cursor.execute(
            "INSERT INTO brands (brand_name) VALUES (%s) ON CONFLICT (brand_name) DO NOTHING RETURNING brand_id",
            (product['brand'],)
        )
        brand_id = cursor.fetchone()
        if brand_id is None:  # если не вернулся brand_id, получаем существующий
            cursor.execute("SELECT brand_id FROM brands WHERE brand_name = %s", (product['brand'],))
            brand_id = cursor.fetchone()[0]
        else:
            brand_id = brand_id[0]

        # Вставляем поставщика, если его нет
        cursor.execute(
            """
            INSERT INTO suppliers (supplier_name, supplier_rating) 
            VALUES (%s, %s) 
            ON CONFLICT (supplier_name) DO NOTHING RETURNING supplier_id
            """,
            (product['supplier'], product['supplierRating'])
        )
        supplier_id = cursor.fetchone()
        if supplier_id is None:
            cursor.execute("SELECT supplier_id FROM suppliers WHERE supplier_name = %s", (product['supplier'],))
            supplier_id = cursor.fetchone()[0]
        else:
            supplier_id = supplier_id[0]

        # Вставляем товар в основную таблицу
        cursor.execute(
            """
            INSERT INTO products (id, name, price, salePriceU, cashback, sale, brand_id, rating, supplier_id, feedbacks, 
            reviewRating, promoTextCard, promoTextCat, link, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                product['id'], product['name'], product['price'], product['salePriceU'],
                product['cashback'], product['sale'], brand_id, product['rating'],
                supplier_id, product['feedbacks'], product['reviewRating'],
                product['promoTextCard'], product['promoTextCat'],
                product['link'], datetime.now()
            )
        )
    conn.commit()

def parser(url, low_price=1, top_price=1000000, discount=0):
    catalog_data = get_data_category(get_catalogs_wb())
    category = search_category_in_catalog(url, catalog_data)
    data_list = []
    for page in range(1, 51):
        data = scrap_page(page, category['shard'], category['query'], low_price, top_price, discount)
        data_list.extend(get_data_from_json(data))
    save_to_db(data_list)
    print(f'Сохранено {len(data_list)} товаров')

if __name__ == '__main__':
    url = 'https://www.wildberries.ru/catalog/sport/vidy-sporta/velosport/velosipedy'
    parser(url=url, low_price=1000, top_price=10000, discount=10)
    cursor.close()
    conn.close()
