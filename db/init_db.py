import psycopg2
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Подключение к базе данных PostgreSQL
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

try:
    # Подключаемся к базе данных
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = connection.cursor()

    # Читаем и выполняем команды из файла schema.sql
    with open("db/schema.sql", "r") as file:
        schema_sql = file.read()
        cursor.execute(schema_sql)
        print("Таблицы успешно созданы в базе данных!")

    # Сохраняем изменения
    connection.commit()

except Exception as error:
    print("Ошибка при подключении к базе данных или выполнении SQL:", error)

finally:
    # Закрываем соединение
    if cursor:
        cursor.close()
    if connection:
        connection.close()

