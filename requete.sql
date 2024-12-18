DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_sellers;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_city VARCHAR,
    customer_state VARCHAR
);

CREATE TABLE dim_products (
    product_id VARCHAR PRIMARY KEY,
    category VARCHAR
);

CREATE TABLE dim_sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_city VARCHAR
);


CREATE TABLE dim_time (
    order_id VARCHAR PRIMARY KEY,
    order_date TIMESTAMP WITH TIME ZONE,
    month VARCHAR,
    year INT,
    quarter VARCHAR
);

CREATE TABLE fact_sales (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    seller_id VARCHAR NOT NULL,
    date_id VARCHAR NOT NULL,
    payment_value DECIMAL NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id),
    FOREIGN KEY (order_id) REFERENCES dim_time(order_id)
);
