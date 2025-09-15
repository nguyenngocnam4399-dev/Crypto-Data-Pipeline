CREATE DATABASE IF NOT EXISTS thesis;
USE thesis;

-- 1. Dimension Table
CREATE TABLE symbol_dim (
    symbol_id INT AUTO_INCREMENT PRIMARY KEY,
    symbol_name VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO symbol_dim (symbol_name)
VALUES ('BTCUSDT'),
('ETHUSDT'),
('BNBUSDT'),
('PEPEUSDT'),
('XRPUSDT'),
('SOLUSDT'),
('DOGEUSDT'),
('LINKUSDT');

select * from symbol_dim;

-- 2. Dimension Table
CREATE TABLE interval_dim (
    interval_id INT AUTO_INCREMENT PRIMARY KEY,
    interval_name VARCHAR(10) NOT NULL UNIQUE
);

INSERT INTO interval_dim (interval_name)
VALUES
('1m'),
('1h'),
('1d');

select * from interval_dim;

-- 3. Fact Table
CREATE TABLE kline_fact (
    kline_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    interval_id INT NOT NULL,
    open_price DECIMAL(20,10) NOT NULL,
    high_price DECIMAL(20,10) NOT NULL,
    low_price DECIMAL(20,10) NOT NULL,
    close_price DECIMAL(20,10) NOT NULL,
    volume DECIMAL(38,18) NOT NULL,
    open_time DATETIME NOT NULL,
    close_time DATETIME NOT NULL,

	FOREIGN KEY (symbol_id) REFERENCES symbol_dim(symbol_id),
    FOREIGN KEY (interval_id) REFERENCES interval_dim(interval_id),
    CONSTRAINT unique_kline UNIQUE(symbol_id, interval_id, open_time)
);

CREATE INDEX idx_kline_time ON kline_fact(symbol_id, interval_id, open_time);

select * FROM kline_fact k
JOIN symbol_dim s ON k.symbol_id = s.symbol_id
JOIN interval_dim i ON k.interval_id = i.interval_id;

select count(*) FROM kline_fact;
select * FROM kline_fact;
