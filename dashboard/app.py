import os
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Параметры подключения к базе данных
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Функция для подключения к базе данных и выполнения запросов
def run_query(query):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        result = pd.read_sql(query, conn)
        conn.close()
        return result
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных или выполнения запроса: {e}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame в случае ошибки

# Название дашборда
st.title("Анализ данных Wildberries")

# Раздел 1: Общее количество товаров и распределение по брендам
st.header("Общее количество товаров и брендов")

# Проверка данных: количество товаров
query_total_products = "SELECT COUNT(*) AS total_products FROM products"
total_products = run_query(query_total_products)
st.write("Общее количество товаров (отладка):", total_products)

# Проверка данных: распределение товаров по брендам
query_brands = """
SELECT b.brand_name, COUNT(p.id) AS product_count
FROM products p
JOIN brands b ON p.brand_id = b.brand_id
GROUP BY b.brand_name
ORDER BY product_count DESC
LIMIT 10;
"""
brands_data = run_query(query_brands)
st.write("Топ 10 брендов по количеству товаров (отладка):")
st.write(brands_data)

# Если данные загружены корректно, выводим метрики и график
if not total_products.empty:
    total_count = total_products.iloc[0]['total_products']
    st.metric("Общее количество товаров", total_count)

if not brands_data.empty:
    st.subheader("Топ 10 брендов по количеству товаров")
    st.bar_chart(brands_data.set_index("brand_name"))

# Раздел 2: Средняя цена и распределение цен
st.header("Средняя цена товаров")

# Проверка данных: средняя цена
query_avg_price = "SELECT AVG(price) AS avg_price FROM products"
avg_price_data = run_query(query_avg_price)
st.write("Средняя цена товаров (отладка):", avg_price_data)

# Проверка данных: распределение цен
query_price_distribution = "SELECT price FROM products WHERE price IS NOT NULL"
price_data = run_query(query_price_distribution)
st.write("Распределение цен (отладка):")
st.write(price_data)

if not avg_price_data.empty:
    avg_price = avg_price_data.iloc[0]['avg_price']
    st.metric("Средняя цена", f"{avg_price:.2f} руб.")

if not price_data.empty:
    st.subheader("Распределение цен")
    # Создаем простой столбчатый график для гистограммы распределения цен
    st.bar_chart(price_data['price'].value_counts().sort_index())

# Раздел 3: Распределение рейтингов
st.header("Распределение рейтингов товаров")
query_ratings = "SELECT rating FROM products WHERE rating IS NOT NULL"
ratings_data = run_query(query_ratings)
st.write("Распределение рейтингов (отладка):")
st.write(ratings_data)

if not ratings_data.empty:
    st.subheader("Распределение рейтингов")
    st.bar_chart(ratings_data['rating'].value_counts().sort_index())

# Раздел 4: Средняя скидка по товарам
st.header("Средняя скидка по товарам")
query_avg_discount = "SELECT AVG(sale) AS avg_discount FROM products WHERE sale IS NOT NULL"
avg_discount_data = run_query(query_avg_discount)
st.write("Средняя скидка (отладка):", avg_discount_data)

if not avg_discount_data.empty:
    avg_discount = avg_discount_data.iloc[0]['avg_discount']
    st.metric("Средняя скидка", f"{avg_discount:.2f}%")
