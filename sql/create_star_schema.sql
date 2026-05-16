-- Product dimension table
CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_price DECIMAL(10, 2)
);

-- Customer dimension table
CREATE TABLE dim_customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(100)
);

-- Store dimension table with auto-generated IDs
CREATE TABLE dim_stores (
    store_id BIGINT PRIMARY KEY,
    store_name VARCHAR(255),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_email VARCHAR(255),
    store_phone VARCHAR(50)
);

-- Supplier dimension table with auto-generated IDs
CREATE TABLE dim_suppliers (
    supplier_id BIGINT PRIMARY KEY,
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(255),
    seller_country VARCHAR(100)
);

-- Fact sales table linking all dimensions
CREATE TABLE fact_sales (
    product_id INT,
    customer_id INT,
    store_id BIGINT,
    supplier_id BIGINT,
    price DECIMAL(10, 2),
    rating DECIMAL(2, 1),
    review_id INT,
    order_date DATE,
    PRIMARY KEY (product_id, customer_id, store_id, supplier_id, order_date)
);

-- Create foreign key constraints
ALTER TABLE fact_sales 
ADD CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
ADD CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
ADD CONSTRAINT fk_supplier FOREIGN KEY (supplier_id) REFERENCES dim_suppliers(supplier_id);

-- Add indexes for better query performance
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_date ON fact_sales(order_date);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_id);
CREATE INDEX idx_fact_sales_supplier ON fact_sales(supplier_id);