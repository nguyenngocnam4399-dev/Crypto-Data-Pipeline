USE thesis;

CREATE TABLE indicator_fact (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    interval_id INT NOT NULL,
    type VARCHAR(10),       -- 'SMA' hoặc 'RSI'
    value DECIMAL(18,8),    -- giá trị chỉ số
    timestamp DATETIME,     -- từ close_time của kline_fact
    FOREIGN KEY (symbol_id) REFERENCES symbol_dim(symbol_id),
    FOREIGN KEY (interval_id) REFERENCES interval_dim(interval_id),
    UNIQUE(symbol_id, interval_id, type, timestamp)  -- đảm bảo atomic, tránh trùng lặp
);
select * FROM indicator_fact i
join symbol_dim s On s.symbol_id = i.symbol_id
join interval_dim d On d.interval_id = i.interval_id
join kline_fact k on k.symbol_id = i.symbol_id and k.interval_id = i.interval_id and k.close_time = i.timestamp;
select count(*) FROM indicator_fact;


