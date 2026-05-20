-- Membuat table source untuk menyimpan data mentah yang akan dilakukan proses ETL/ELT

-- table authors
CREATE OR REPLACE TABLE authors (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table articles 
CREATE OR REPLACE TABLE articles (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    published_at TIMESTAMP,
    author_id BIGINT not null,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP default NULL,
    CONSTRAINT fk_author
        FOREIGN KEY (author_id) 
        REFERENCES authors(id)
);

-- Membuat Index untuk kebutuhan percepatan Incremental Load
CREATE INDEX idx_articles_published_at ON articles(published_at);
CREATE INDEX idx_articles_updated_at ON articles(updated_at);
CREATE INDEX idx_articles_deleted_at ON articles(deleted_at);

CREATE INDEX idx_authors_updated_at ON authors(updated_at);

-- Membuat Function auto update updated_at pada table articles
CREATE OR REPLACE FUNCTION updateArticles_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Membuat Function auto update updated_at pada table authors
CREATE OR REPLACE FUNCTION updateAuthors_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Membuat Trigger untuk auto update updated_at pada table articles
CREATE TRIGGER trigger_update_updated_at
BEFORE UPDATE ON articles
FOR EACH ROW
EXECUTE FUNCTION updateArticles_updated_at_column();

-- Membuat Trigger untuk auto update updated_at pada table authors
CREATE TRIGGER trigger_update_authors_updated_at
BEFORE UPDATE ON authors
FOR EACH ROW
EXECUTE FUNCTION updateAuthors_updated_at_column();