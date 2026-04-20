-- scripts/seed-data.sql
-- ======================
-- Seed data for the PostgreSQL source database (seed_db).
--
-- Creates two tables with realistic e-commerce data used as a source
-- for the Airbyte PostgreSQL connector in the Medallion Pipeline:
--
--   customers : 10 sample customers across US cities
--   orders    : 13 sample orders with various statuses and amounts
--
-- Both tables include:
--   - created_at / updated_at timestamps for incremental sync support
--   - Indexes on updated_at for efficient cursor-based incremental queries
--
-- This script runs automatically on PostgreSQL container first start via:
--   /docker-entrypoint-initdb.d/seed-data.sql
--
-- To reload manually:
--   docker exec postgres psql -U postgres -d seed_db -f /docker-entrypoint-initdb.d/seed-data.sql

\c seed_db;

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, phone, city, state, country, created_at, updated_at) VALUES
('John', 'Doe', 'john.doe@email.com', '+1-555-0101', 'New York', 'NY', 'USA', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('Jane', 'Smith', 'jane.smith@email.com', '+1-555-0102', 'Los Angeles', 'CA', 'USA', '2024-01-02 11:00:00', '2024-01-02 11:00:00'),
('Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-0103', 'Chicago', 'IL', 'USA', '2024-01-03 12:00:00', '2024-01-03 12:00:00'),
('Alice', 'Williams', 'alice.williams@email.com', '+1-555-0104', 'Houston', 'TX', 'USA', '2024-01-04 13:00:00', '2024-01-04 13:00:00'),
('Charlie', 'Brown', 'charlie.brown@email.com', '+1-555-0105', 'Phoenix', 'AZ', 'USA', '2024-01-05 14:00:00', '2024-01-05 14:00:00'),
('Diana', 'Davis', 'diana.davis@email.com', '+1-555-0106', 'Philadelphia', 'PA', 'USA', '2024-01-06 15:00:00', '2024-01-06 15:00:00'),
('Eve', 'Miller', 'eve.miller@email.com', '+1-555-0107', 'San Antonio', 'TX', 'USA', '2024-01-07 16:00:00', '2024-01-07 16:00:00'),
('Frank', 'Wilson', 'frank.wilson@email.com', '+1-555-0108', 'San Diego', 'CA', 'USA', '2024-01-08 17:00:00', '2024-01-08 17:00:00'),
('Grace', 'Moore', 'grace.moore@email.com', '+1-555-0109', 'Dallas', 'TX', 'USA', '2024-01-09 18:00:00', '2024-01-09 18:00:00'),
('Henry', 'Taylor', 'henry.taylor@email.com', '+1-555-0110', 'San Jose', 'CA', 'USA', '2024-01-10 19:00:00', '2024-01-10 19:00:00');

-- Insert sample orders
INSERT INTO orders (customer_id, order_date, order_status, total_amount, payment_method, shipping_address, created_at, updated_at) VALUES
(1, '2024-01-15', 'completed', 125.50, 'credit_card', '123 Main St, New York, NY 10001', '2024-01-15 10:30:00', '2024-01-15 10:30:00'),
(1, '2024-02-20', 'completed', 89.99, 'credit_card', '123 Main St, New York, NY 10001', '2024-02-20 14:15:00', '2024-02-20 14:15:00'),
(2, '2024-01-18', 'completed', 250.00, 'paypal', '456 Oak Ave, Los Angeles, CA 90001', '2024-01-18 11:00:00', '2024-01-18 11:00:00'),
(2, '2024-03-05', 'shipped', 175.25, 'credit_card', '456 Oak Ave, Los Angeles, CA 90001', '2024-03-05 09:45:00', '2024-03-05 09:45:00'),
(3, '2024-01-22', 'completed', 99.99, 'debit_card', '789 Elm St, Chicago, IL 60601', '2024-01-22 16:20:00', '2024-01-22 16:20:00'),
(4, '2024-02-10', 'completed', 450.00, 'credit_card', '321 Pine Rd, Houston, TX 77001', '2024-02-10 13:30:00', '2024-02-10 13:30:00'),
(4, '2024-03-12', 'processing', 320.75, 'paypal', '321 Pine Rd, Houston, TX 77001', '2024-03-12 10:00:00', '2024-03-12 10:00:00'),
(5, '2024-02-14', 'completed', 75.50, 'credit_card', '654 Maple Dr, Phoenix, AZ 85001', '2024-02-14 15:45:00', '2024-02-14 15:45:00'),
(6, '2024-02-28', 'completed', 199.99, 'debit_card', '987 Cedar Ln, Philadelphia, PA 19101', '2024-02-28 12:00:00', '2024-02-28 12:00:00'),
(7, '2024-03-08', 'shipped', 135.00, 'credit_card', '147 Birch Ct, San Antonio, TX 78201', '2024-03-08 14:30:00', '2024-03-08 14:30:00'),
(8, '2024-03-15', 'completed', 89.50, 'paypal', '258 Spruce Way, San Diego, CA 92101', '2024-03-15 11:15:00', '2024-03-15 11:15:00'),
(9, '2024-03-18', 'processing', 275.00, 'credit_card', '369 Willow Blvd, Dallas, TX 75201', '2024-03-18 16:00:00', '2024-03-18 16:00:00'),
(10, '2024-03-20', 'completed', 425.99, 'debit_card', '741 Ash St, San Jose, CA 95101', '2024-03-20 10:45:00', '2024-03-20 10:45:00');

-- Create indexes for better query performance
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_updated_at ON customers(updated_at);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);

-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;

-- Display summary
SELECT 'Seed data loaded successfully' AS status;
SELECT COUNT(*) AS customer_count FROM customers;
SELECT COUNT(*) AS order_count FROM orders;
