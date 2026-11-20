CREATE TABLE IF NOT EXISTS purchase_orders (
    order_number VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    snapshot JSON NOT NULL,
    PRIMARY KEY (order_number)
) ENGINE=InnoDB;
