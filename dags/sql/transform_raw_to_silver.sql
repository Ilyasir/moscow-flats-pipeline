-- Этот SQL один из главных в пайплайне, убирает дубли по адрес+этаж+кол-во комнат
-- убирает фейки, делает типизацию, регулярками вытаскивает ваажные параметры,
-- обогащает доп. признаками, типо студия или апартаменты, новая москва или нет, мин до метро и тд
-- и в итоге сохраняет в паркет в S3
COPY (
    WITH raw_transformed AS (
        SELECT
            id::BIGINT as id,
            SPLIT_PART(link, '?', 1)::TEXT as link, -- укорачиваем ссылку
            title::VARCHAR as title,
            -- доп признаки из title
            CASE WHEN title ILIKE '%апартаменты%' THEN TRUE ELSE FALSE END as is_apartament,
            CASE WHEN title ILIKE '%студия%' THEN TRUE ELSE FALSE END as is_studio,
            -- вытаскиваем площадь, удаляем пробелы, запятую на точку меняем
            replace(
                regexp_replace(
                    NULLIF(regexp_extract(title, '([\d\s]+[.,]?\d*)\s*м²', 1), ''),
                    '\s+', '', 'g'
                ),
                ',', '.'
            )::NUMERIC(10, 2) AS area,
            -- кол-во комнат (0 для студий)
            CASE 
                WHEN title ILIKE '%студия%' THEN 0
                ELSE NULLIF(regexp_extract(title, '^(\d+)', 1), '')::INT
            END as rooms_count,
            -- этажи
            NULLIF(regexp_extract(title, '(\d+)/\d+\s*(?:этаж|эт\.)', 1), '')::INT as floor,
            NULLIF(regexp_extract(title, '\d+/(\d+)\s*(?:этаж|эт\.)', 1), '')::INT as total_floors,
            -- для цены убираем знак рубля, пробелы
            regexp_replace(price, '[^0-9]', '', 'g')::BIGINT as price,
            -- география (адрес, округ, район)
            address::TEXT as address,
            SPLIT_PART(address, ',', 1)::VARCHAR as city,
            NULLIF(regexp_extract(address, '([А-Яа-я]+АО)', 1), '')::VARCHAR as okrug,
            -- район, для новой москвы ставим округ, слишком нестабильно (районов там по сути нету)
            CASE
                WHEN okrug IN ('НАО', 'ТАО') THEN okrug
                ELSE 
                    regexp_replace(
                        NULLIF(regexp_extract(address, '(р-н\s?[^,]+)', 1), ''), 
                        '^р-н\s*', '', 'i'
                    )::VARCHAR
            END as district,
            CASE WHEN okrug IN ('НАО', 'ТАО') THEN TRUE ELSE FALSE END as is_new_moscow,
            -- инфа о метро
            NULLIF(regexp_extract(metro, '^(.*?)\d+\s+минут', 1), '')::VARCHAR as metro_name,
            NULLIF(regexp_extract(metro, '(\d+)\s+минут', 1), '')::INT as metro_min,
            CASE
                WHEN metro LIKE '%пешком%' THEN 'walk'
                WHEN metro LIKE '%транс%' THEN 'transport'
            END as metro_type,
            parsed_at::TIMESTAMP as parsed_at,
            -- огромный текст с описанием, может пригодится
            description::TEXT as description,
            -- поля ток для дедубликации, уникальный хэш квартиры
            lower(regexp_replace(address, '[^а-яА-Я0-9]', '', 'g')) as norm_address,
            md5(concat_ws('|', norm_address, floor, total_floors, rooms_count)) as flat_hash
        FROM read_json_auto('{{ raw_s3_key }}')
    ),
    -- убираем дубли, с оконной функцией нумеруем записи по убыванию даты
    deduplicated AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY norm_address, floor, total_floors, rooms_count
                ORDER BY parsed_at DESC, id DESC
            ) as row_num
        FROM raw_transformed
        -- отсекаем фейки, битые обьявления без заголовков
        WHERE area IS NOT NULL 
            AND price IS NOT NULL
            AND okrug IS NOT NULL
            AND rooms_count IS NOT NULL
            AND floor IS NOT NULL
            AND total_floors IS NOT NULL
            AND (district IS NOT NULL OR is_new_moscow) -- у новой москвы района может не быть
            AND price / NULLIF(area, 0) > 50000 -- на всякий, мало ли 0 в площади
    )
    SELECT * EXCLUDE (row_num, norm_address) -- EXCLUDE в duckdb (можно не перечислять все поля)
    FROM deduplicated
    WHERE row_num = 1 -- осталяем только первую запись, самую новую по парсингу
) TO '{{ silver_s3_key }}' (FORMAT PARQUET, OVERWRITE TRUE);