import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.datasets import GOLD_DATASET_HISTORY
from utils.telegram import on_failure_callback

OWNER = "ilyas"
DAG_ID = "gold_marts_current_stats"

SHORT_DESCRIPTION = "Расчет витрин для текущего состояния рынка недвижимости по районам и метро"

LONG_DESCRIPTION = """
Этот DAG агрегирует данные из исторической таблицы `history_flats` и формирует актуальные витрины.

### Витрины:
1. **dm_district_current**: Сводная статистика по районам (средняя цена, медиана за метр, кол-во квартир).
2. **dm_metro_current**: Статистика по станциям метро. Включает только станции с 20+ объявлениями,
чтобы было статистичеки верно. Также есть среднее расстояние в минутах пешком.
3. **dm_price_drops**: квартиры, на которые цена снизилась более чем на 100к относительно предыдущей записи.

Запускается автоматически после обновления датасета `GOLD_DATASET_HISTORY`.
Перезаписывается полность, тоесть юзается `TRUNCATE` перед вставкой, так как витрина показывает только текущие квартиры
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": on_failure_callback,
}


with DAG(
    dag_id=DAG_ID,
    schedule=[GOLD_DATASET_HISTORY],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["marts", "gold", "pg"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    build_dm_district_current = SQLExecuteQueryOperator(
        task_id="build_dm_district_current",
        conn_id="pg_conn",
        autocommit=True,
        sql="""
            TRUNCATE TABLE gold.dm_district_current; -- фулл очистка перед вставкой 

            INSERT INTO gold.dm_district_current (
                okrug, district, total_flats, avg_price, avg_price_per_meter, 
                median_price_per_meter, min_price, max_price
            )
            SELECT
                okrug,
                district,
                count(*) as total_flats,
                round(avg(price)) as avg_price,
                round(avg(price / area)) as avg_price_per_meter,
                -- для цен медиана точнее 
                round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price / area))::BIGINT as median_price_per_meter,
                min(price) as min_price,
                max(price) as max_price
            FROM gold.history_flats
            WHERE is_active = true -- ток активные квартиры
            GROUP BY okrug, district;
        """,
    )

    build_dm_metro_current = SQLExecuteQueryOperator(
        task_id="build_dm_metro_current",
        conn_id="pg_conn",
        autocommit=True,
        sql="""
            TRUNCATE TABLE gold.dm_metro_current;

            INSERT INTO gold.dm_metro_current(
                metro_name, total_flats, avg_price,
                avg_price_per_meter, median_price_per_meter,
                avg_walking_min
            )
            with metro_table as (
                select
                    metro_name,
                    count(*) as total_flats,
                    round(avg(price)) as avg_price,
                    round(avg(price / area)) as avg_price_per_meter,
                    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price / area))::BIGINT as median_price_per_meter,
                    -- считаем среднее время пешком только для walk
                    round(avg(
                            case
                                WHEN metro_type = 'walk'
                                THEN metro_min
                            END), 2) as avg_walking_min
                from gold.history_flats as hf
                where hf.metro_name is not null and is_active = true
                group by hf.metro_name)
                select
                    metro_name, total_flats, avg_price,
                    avg_price_per_meter, median_price_per_meter,
                    avg_walking_min
                from metro_table
                -- Отсекаем редкие станции и квартиры где вообще нету мтеро
                where total_flats > 20
                    and avg_walking_min is not null
        """,
    )

    build_dm_price_drops = SQLExecuteQueryOperator(
        task_id="build_dm_price_drops",
        conn_id="pg_conn",
        autocommit=True,
        sql="""
            TRUNCATE TABLE gold.dm_price_drops;

            INSERT INTO gold.dm_price_drops (
                flat_hash, link, district, area, rooms_count, 
                old_price, new_price, drop_percent, drop_abs
            )
            with old_and_new_price as (select
                flat_hash, link, district, area, rooms_count,
                LAG(price, 1, price) over(partition by flat_hash ORDER BY effective_from) as old_price,
                price as new_price,
                is_active
            from gold.history_flats)
            select
                flat_hash, link, district, area, rooms_count,
                old_price, new_price,
                round((old_price - new_price) * 100.0 / old_price, 2) AS drop_percent,
                (old_price - new_price) as drop_abs
            from old_and_new_price
            where new_price < old_price
                and is_active = true
                and (old_price - new_price) > 100000
            order by drop_percent desc
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> [build_dm_district_current, build_dm_metro_current, build_dm_price_drops] >> end  # паралельно
