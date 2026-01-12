
-- Dimension Tables

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    segment VARCHAR(50),
    region VARCHAR(50)
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100)
);

CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    -- Enriched fields
    population INT,
    median_income DECIMAL(12, 2)
);

-- Fact Table

CREATE TABLE fact_sales (
    sale_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    product_key INT REFERENCES dim_product(product_key),
    location_key INT REFERENCES dim_location(location_key),

    quantity INT,
    sales_amount DECIMAL(12, 2),
    discount DECIMAL(5, 4),
    profit DECIMAL(12, 2),
    shipping_cost DECIMAL(10, 2)
);

-- Indexes for performance
CREATE INDEX idx_fact_date ON fact_sales(date_key);
CREATE INDEX idx_fact_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_product ON fact_sales(product_key);
CREATE INDEX idx_fact_location ON fact_sales(location_key);
