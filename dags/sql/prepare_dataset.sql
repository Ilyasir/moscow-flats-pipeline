COPY (
    WITH base_table as (
        select
            -- самые свежие записи, убираем дубли со старыми ценами
            row_number() OVER(
                partition by flat_hash
                order by effective_to desc
            ) as row_num,
            round((price / area)) as price_per_meter,
            is_apartament,
            is_studio,
            area::DOUBLE as area,
            rooms_count,
            floor,
            total_floors,
            (floor = 1) as is_first_floor,
            (floor = total_floors) as is_last_floor,
            is_new_moscow,
            okrug,
            district,
            -- нормализация времени до метро
            CASE
                WHEN metro_type = 'walk' THEN metro_min
                ELSE metro_min * 5 -- примерно, умнножаем на 5 для трансопрта
            END as metro_min,
            flat_hash
        from flats_db.gold.history_flats
        where metro_min is not null
    )
    select
        * EXCLUDE (row_num, flat_hash),
        -- доп. признаки для модели 
        (floor::DOUBLE / total_floors::DOUBLE) as rel_floor,
        (area / (rooms_count + 1)) as area_per_room,
        (total_floors > 18) as is_high_rise
    from base_table
    where row_num = 1
    -- чтобы порядок в датасете был стабильный, иначе метрики скачут на тех же данных
    order by flat_hash
) TO '{{ dataset_s3_key }}' (FORMAT PARQUET);