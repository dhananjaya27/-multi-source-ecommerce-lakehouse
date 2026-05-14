INSERT INTO customers (first_name, last_name, email, phone, city, state, country)
VALUES
('Aarav', 'Sharma', 'aarav.sharma@example.com', '9876543210', 'Bangalore', 'Karnataka', 'India'),
('Priya', 'Patel', 'priya.patel@example.com', '9876543211', 'Mumbai', 'Maharashtra', 'India'),
('Rohan', 'Das', 'rohan.das@example.com', '9876543212', 'Delhi', 'Delhi', 'India');

INSERT INTO orders (customer_id, order_status, total_amount, city, state)
VALUES
(1, 'PLACED', 2499.00, 'Bangalore', 'Karnataka'),
(2, 'PLACED', 1599.00, 'Mumbai', 'Maharashtra'),
(3, 'CONFIRMED', 3499.00, 'Delhi', 'Delhi');

INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_amount, item_total)
VALUES
(1, 101, 1, 2499.00, 0.00, 2499.00),
(2, 102, 2, 899.00, 199.00, 1599.00),
(3, 103, 1, 3999.00, 500.00, 3499.00);

INSERT INTO payments (order_id, payment_method, payment_status, payment_amount, transaction_id)
VALUES
(1, 'UPI', 'SUCCESS', 2499.00, 'TXN1001'),
(2, 'CARD', 'SUCCESS', 1599.00, 'TXN1002'),
(3, 'UPI', 'PENDING', 3499.00, 'TXN1003');