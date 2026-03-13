-- Test database initialization
-- This script is automatically executed when the MySQL container starts

USE ds_mysql_test;

-- Create symbol table if it doesn't exist
CREATE TABLE IF NOT EXISTS symbol (
  id int unsigned NOT NULL AUTO_INCREMENT,
  exchange_id int DEFAULT NULL,
  ticker varchar(32) NOT NULL,
  instrument varchar(64) NOT NULL,
  name varchar(255) DEFAULT NULL,
  sector varchar(255) DEFAULT NULL,
  currency varchar(32) DEFAULT NULL,
  created_date datetime NOT NULL,
  last_updated_date datetime NOT NULL,
  PRIMARY KEY (id),
  KEY index_exchange_id (exchange_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert test data
INSERT IGNORE INTO symbol (
  id,
  exchange_id,
  ticker,
  instrument,
  name,
  sector,
  currency,
  created_date,
  last_updated_date
) VALUES
    (1, NULL, 'AAPL', 'Equity', 'Apple Inc.', 'Technology', 'USD', NOW(), NOW()),
    (2, NULL, 'GOOGL', 'Equity', 'Alphabet Inc.', 'Technology', 'USD', NOW(), NOW()),
    (3, NULL, 'MSFT', 'Equity', 'Microsoft Corporation', 'Technology', 'USD', NOW(), NOW()),
    (4, NULL, 'AMZN', 'Equity', 'Amazon.com Inc.', 'Consumer Discretionary', 'USD', NOW(), NOW()),
    (5, NULL, 'TSLA', 'Equity', 'Tesla Inc.', 'Consumer Discretionary', 'USD', NOW(), NOW());
