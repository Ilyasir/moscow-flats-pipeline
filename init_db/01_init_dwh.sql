-- DDL
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TYPE gold.transport_type AS ENUM('walk', 'transport');
CREATE TYPE gold.okrug_name AS ENUM('НАО', 'ТАО', 'ЦАО', 'САО', 'ЮАО', 'ЗАО', 'ВАО', 'ЮЗАО', 'ЮВАО', 'СЗАО', 'СВАО', 'ЗелАО');

CREATE TABLE IF NOT EXISTS gold.history_flats (
	id BIGSERIAL PRIMARY KEY, -- суррогатный ключ
	flat_hash CHAR(32) not null, -- уникальный хэш квартиры (md5 от адреса, этажа и комнатности)
	link TEXT not null,
	title VARCHAR(100) not null,
	price BIGINT not null CHECK (price > 0), -- в москве цены огромные, юзаем bigint
	-- характеристики квартиры
	is_apartament BOOLEAN not null,
	is_studio BOOLEAN not null,
	area NUMERIC(10, 2) not null,
	rooms_count INT not null,
	floor INT not null,
	total_floors INT not null,
	СHECK (floor <= total_floors),
	-- геоданные
	is_new_moscow BOOLEAN not null,
	address TEXT not null,
	city VARCHAR(50) not null,
	okrug gold.okrug_name not null,
	district VARCHAR(100),
	-- инфа о метро
	metro_name VARCHAR(100),
	metro_min INT,
	metro_type gold.transport_type,
	-- тех. поля для истории (SCD2)
	effective_from TIMESTAMP not null,
    effective_to TIMESTAMP not null DEFAULT '9999-12-31 23:59:59',
    is_active BOOLEAN not null DEFAULT TRUE
);

-- временная таблица, всегда полностью перезаписывается перед мержем в историю
CREATE TABLE IF NOT EXISTS gold.stage_flats (
    flat_hash CHAR(32) not null,
    link TEXT,
    title VARCHAR(100),
    price BIGINT not null,
    is_apartament BOOLEAN,
    is_studio BOOLEAN,
    area NUMERIC(10, 2) not null,
    rooms_count INT,
    floor INT,
    total_floors INT,
    is_new_moscow BOOLEAN,
    address TEXT,
    city VARCHAR(50),
    okrug gold.okrug_name,
    district VARCHAR(100),
    metro_name VARCHAR(100),
    metro_min INT,
    metro_type gold.transport_type,
    parsed_at TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_history_flats_active_hash 
ON gold.history_flats (flat_hash) 
WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_history_flats_period 
ON gold.history_flats (effective_from, effective_to);

CREATE INDEX IF NOT EXISTS idx_history_flats_district 
ON gold.history_flats (okrug, district);