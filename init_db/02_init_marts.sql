CREATE TABLE IF NOT EXISTS gold.dm_district_current (
    okrug gold.okrug_name,
    district VARCHAR(100),
    total_flats INT,
	avg_price BIGINT,
    avg_price_per_meter BIGINT,
    median_price_per_meter BIGINT,
	min_price BIGINT,
	max_price BIGINT,
	updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dm_metro_current (
    metro_name VARCHAR(100) NOT NULL,
    total_flats INT,
    avg_price BIGINT,
    avg_price_per_meter BIGINT,
    median_price_per_meter BIGINT,
    avg_walking_min NUMERIC(4, 2),
	updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dm_district_history (
    report_date DATE NOT NULL,
    okrug gold.okrug_name,
    district VARCHAR(100),
    total_flats INT,
    avg_price_per_meter BIGINT,
    median_price_per_meter BIGINT,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, okrug, district)
);

CREATE TABLE IF NOT EXISTS gold.dm_price_drops (
    flat_hash CHAR(32) NOT NULL,
    link TEXT,
    district VARCHAR(100),
    area NUMERIC(10, 2),
	rooms_count INT,
    old_price BIGINT,
    new_price BIGINT,
    drop_percent NUMERIC(6, 2),
    drop_abs BIGINT,
    updated_at TIMESTAMP DEFAULT NOW()
);