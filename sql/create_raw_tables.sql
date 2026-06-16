-- ============================================================
-- Mandera Analytics - Database Schema
-- Raw landing tables and staging tables
-- ============================================================

-- Raw transactions table (landing zone)
CREATE TABLE IF NOT EXISTS raw_transactions (
    id                  SERIAL PRIMARY KEY,
    transaction_id      VARCHAR(100),
    batch_id            VARCHAR(100),
    customer_id         VARCHAR(50),
    customer_name       VARCHAR(200),
    customer_email      VARCHAR(200),
    region              VARCHAR(50),
    product_id          VARCHAR(50),
    product_name        VARCHAR(200),
    quantity            INTEGER,
    unit_price          NUMERIC(10,2),
    payment_method      VARCHAR(50),
    status              VARCHAR(50),
    transaction_date    TIMESTAMP,
    created_at          TIMESTAMP,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Batch tracking table
CREATE TABLE IF NOT EXISTS batch_log (
    id                  SERIAL PRIMARY KEY,
    batch_id            VARCHAR(100) UNIQUE,
    source              VARCHAR(50),
    row_count           INTEGER,
    expected_count      INTEGER,
    variance            INTEGER,
    status              VARCHAR(50),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging transactions table (clean data)
CREATE TABLE IF NOT EXISTS stg_transactions (
    id                  SERIAL PRIMARY KEY,
    transaction_id      VARCHAR(100) UNIQUE,
    batch_id            VARCHAR(100),
    customer_id         VARCHAR(50),
    customer_name       VARCHAR(200),
    customer_email      VARCHAR(200),
    region              VARCHAR(50),
    product_id          VARCHAR(50),
    product_name        VARCHAR(200),
    quantity            INTEGER,
    unit_price          NUMERIC(10,2),
    total_amount        NUMERIC(10,2),
    payment_method      VARCHAR(50),
    status              VARCHAR(50),
    transaction_date    DATE,
    created_at          TIMESTAMP,
    transformed_at      TIMESTAMP DEFAULT NOW()
);
