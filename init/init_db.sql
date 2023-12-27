-- Dimension Table: Authors
CREATE TABLE IF NOT EXISTS authors (
    author_id SERIAL PRIMARY KEY,
    last_name VARCHAR(255),
    first_name VARCHAR(255),
    middle_name VARCHAR(255),
    UNIQUE (last_name, first_name, middle_name)
);

-- Dimension Table: Categories
CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) UNIQUE NOT NULL
);

-- Dimension Table: Versions
CREATE TABLE IF NOT EXISTS versions (
    version_id SERIAL PRIMARY KEY,
    version VARCHAR(50),
    created_date TIMESTAMP
);

-- Fact Table: Papers
CREATE TABLE IF NOT EXISTS papers (
    paper_id VARCHAR(50) PRIMARY KEY,
    title TEXT NOT NULL,
    submitter VARCHAR(255),
    comments TEXT,
    journal_ref TEXT,
    doi TEXT,
    report_no VARCHAR(255),
    abstract TEXT,
    update_date DATE,
    citation_count INT DEFAULT 0
);

-- Junction Table for Papers and Authors Many-to-Many Relationship
CREATE TABLE IF NOT EXISTS paper_authors (
    paper_id VARCHAR(50),
    author_id INT,
    PRIMARY KEY (paper_id, author_id),
    FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);

-- Junction Table for Papers and Categories Many-to-Many Relationship
CREATE TABLE IF NOT EXISTS paper_categories (
    paper_id VARCHAR(50),
    category_id INT,
    PRIMARY KEY (paper_id, category_id),
    FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- Remember to add indexes for frequently queried columns for better performance, e.g.,
CREATE INDEX IF NOT EXISTS idx_authors_last_name ON authors(last_name);
CREATE INDEX IF NOT EXISTS idx_categories_name ON categories(category_name);
CREATE INDEX IF NOT EXISTS idx_papers_update_date ON papers(update_date);

--
-- The paper_id in the papers table is a VARCHAR to accommodate the ID format from the JSON data.
-- The authors_parsed from the JSON is broken down into first_name, middle_name, and last_name in the authors table.
-- The versions table in this schema is simplified and does not include a direct relationship with the papers table, as it seems that versions are attributes of the papers, not independent entities. If the versions are to be connected to papers, you would need to include a paper_id foreign key in the versions table and establish the necessary relationships.
-- Depending on the JSON structure and how you want to handle the categories, you might need to preprocess this data to split it into separate category names before inserting it into the categories table.