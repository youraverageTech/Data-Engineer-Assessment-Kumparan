-- Membuat table target untuk menyimpan data hasil proses ETL/ELT

-- Membuat schema staging untuk menyimpan data raw yang sudah di ekstrak dari source
CREATE OR REPLACE SCHEMA staging;
-- table authors
CREATE OR REPLACE TABLE staging.authors (
    authors_id VARCHAR NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
-- Table articles
CREATE OR REPLACE TABLE staging.articles (
    articles_id VARCHAR NOT NULL,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    published_at TIMESTAMP,
    author_id VARCHAR not null,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

-- Membuat schema dwh untuk menyimpan table target
CREATE OR REPLACE SCHEMA dwh;
-- table dim_authors
CREATE OR REPLACE TABLE dwh.dim_authors (
    authors_sk NUMBER AUTOINCREMENT,
    authors_id VARCHAR NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- table dim_articles
CREATE OR REPLACE TABLE dwh.dim_articles (
    articles_sk NUMBER AUTOINCREMENT,
    articles_id VARCHAR NOT NULL,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    published_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

-- table dim_date
CREATE OR REPLACE TABLE dwh.dim_date (
    date_sk NUMBER,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT
);

-- table fact_reports_articles
CREATE OR REPLACE TABLE dwh.fact_reports_articles (
    article_sk NUMBER,
    author_sk NUMBER,
    published_date_sk NUMBER,
    article_count NUMBER DEFAULT 1
);