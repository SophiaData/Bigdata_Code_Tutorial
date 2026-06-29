-- Additional test data for performance testing
INSERT INTO users (name, email, age) VALUES 
('David', 'david@example.com', 28),
('Eve', 'eve@example.com', 32),
('Frank', 'frank@example.com', 29);

INSERT INTO orders (user_id, product_name, amount, status) VALUES 
(4, 'Headphones', 199.99, 'completed'),
(4, 'Webcam', 89.99, 'shipped'),
(5, 'Tablet', 399.99, 'pending');

-- Performance test data (1000 records)
-- This will be used for load testing
SET @i = 6;
WHILE @i <= 1000 DO
    INSERT INTO users (name, email, age) 
    VALUES (CONCAT('User', @i), CONCAT('user', @i, '@example.com'), FLOOR(18 + RAND() * 50));
    
    SET @user_id = LAST_INSERT_ID();
    SET @j = 1;
    WHILE @j <= 5 DO
        INSERT INTO orders (user_id, product_name, amount, status)
        VALUES (@user_id, CONCAT('Product', @j), FLOOR(10 + RAND() * 500), 
               ELT(@j, 'completed', 'shipped', 'pending', 'processing', 'cancelled'));
        SET @j = @j + 1;
    END WHILE;
    
    SET @i = @i + 1;
END WHILE;
