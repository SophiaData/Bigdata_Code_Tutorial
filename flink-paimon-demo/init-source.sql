-- Source database schema for MySQL → Paimon sync demo

CREATE DATABASE IF NOT EXISTS source_db;
USE source_db;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    age INT,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id BIGINT NOT NULL AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    product_name VARCHAR(200),
    amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Products table
CREATE TABLE IF NOT EXISTS products (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2),
    stock INT DEFAULT 0,
    category VARCHAR(100),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert sample data
INSERT INTO users (name, email, age) VALUES
('Alice', 'alice@example.com', 30),
('Bob', 'bob@example.com', 25),
('Charlie', 'charlie@example.com', 35);

INSERT INTO products (name, price, stock, category) VALUES
('Laptop', 999.99, 50, 'Electronics'),
('Phone', 699.99, 100, 'Electronics'),
('Book', 29.99, 200, 'Education');

INSERT INTO orders (user_id, product_name, amount, status) VALUES
(1, 'Laptop', 999.99, 'completed'),
(2, 'Phone', 699.99, 'pending'),
(1, 'Book', 29.99, 'completed');
