-- Таблица для брендов
CREATE TABLE IF NOT EXISTS brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(255) UNIQUE NOT NULL
);

-- Таблица для поставщиков
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255) UNIQUE NOT NULL,
    supplier_rating NUMERIC
);

-- Основная таблица для товаров
CREATE TABLE IF NOT EXISTS products (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    price NUMERIC,
    salePriceU NUMERIC,
    cashback INTEGER,
    sale INTEGER,
    brand_id INTEGER REFERENCES brands(brand_id),
    rating NUMERIC,
    supplier_id INTEGER REFERENCES suppliers(supplier_id),
    feedbacks INTEGER,
    reviewRating NUMERIC,
    promoTextCard TEXT,
    promoTextCat TEXT,
    link TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
