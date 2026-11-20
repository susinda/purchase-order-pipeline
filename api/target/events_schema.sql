CREATE TABLE IF NOT EXISTS purchase_order_events (
    event_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (event_id)
) ENGINE=InnoDB;
