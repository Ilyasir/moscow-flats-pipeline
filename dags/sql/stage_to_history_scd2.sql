-- Этот SQL реализует SCD2, сохраняет фулл историю изменений цены
-- закрываем запись, если квартиры нет в stage, то есть она пропала из выдачи парсинга (продалась или снялась)
UPDATE gold.history_flats hf
SET 
    effective_to = (SELECT MAX(parsed_at) FROM gold.stage_flats),
    is_active = FALSE
WHERE hf.is_active = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM gold.stage_flats stg 
      WHERE stg.flat_hash = hf.flat_hash
  );

-- закрываем старые записи со старой ценой, если цена поменялась
UPDATE gold.history_flats as hf
SET
    effective_to = stg.parsed_at,
    is_active = FALSE
FROM gold.stage_flats as stg
WHERE hf.flat_hash = stg.flat_hash
    AND hf.is_active = TRUE
    AND hf.price != stg.price;

-- вставляем квартиры, которые вообще не были в истории, или которые были, но уже с новой ценой
INSERT INTO gold.history_flats (
    flat_hash, link, title, price, is_apartament, is_studio, area, 
    rooms_count, floor, total_floors, is_new_moscow, address, 
    city, okrug, district, metro_name, metro_min, metro_type, 
    effective_from, is_active
)
SELECT 
    stg.flat_hash, stg.link, stg.title, stg.price, stg.is_apartament, stg.is_studio, stg.area, 
    stg.rooms_count, stg.floor, stg.total_floors, stg.is_new_moscow, stg.address, 
    stg.city, stg.okrug, stg.district, stg.metro_name, stg.metro_min, stg.metro_type,
    stg.parsed_at as effective_from,
    TRUE as is_active
FROM gold.stage_flats as stg
WHERE NOT EXISTS (
    SELECT 1 FROM gold.history_flats hf
    WHERE hf.flat_hash = stg.flat_hash AND hf.is_active = TRUE
);
-- total_in_stage и active_after_merge должны совпадать
SELECT -- пуш в xcom
    (SELECT count(*) FROM gold.stage_flats) as total_in_stage,
    (SELECT count(*) FROM gold.history_flats WHERE is_active = TRUE) as active_after_merge,
    (SELECT count(*) FROM gold.history_flats) as total_in_history;