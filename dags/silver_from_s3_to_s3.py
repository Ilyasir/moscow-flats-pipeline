import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from utils.datasets import RAW_DATASET_SALES_FLATS, SILVER_DATASET_SALES_FLATS
from utils.duckdb import connect_duckdb_to_s3, load_sql
from utils.telegram import on_failure_callback, on_success_callback

OWNER = "ilyas"
DAG_ID = "silver_from_s3_to_s3"

LAYER_SOURCE = "raw"
LAYER_TARGET = "silver"

SHORT_DESCRIPTION = "Трансформация, очистка и типизация данных из JSONL в Parquet с применением бизнес-логики."

LONG_DESCRIPTION = """
## DAG: Silver Layer Transformation
Процесс преобразует грязные сырые данные в структурированный формат **Parquet**.

### Логика трансформации (DuckDB SQL):
- **Типизация**: Приведение цен к `BIGINT`, площади к `NUMERIC`, дат к `TIMESTAMP` и т.д.
- **Парсинг Title**: Извлечение площади, этажности, комнатности и типа жилья через регулярки.
- **Гео-данные**: Разбор адреса на город, округ и район. Для новой москвы район может быть null
- **Транспорт**: Категоризация доступности метро (пешком/транспорт) и расчет времени до метро.
- **Очистка**:
    - Удаление дублей по бизнес-ключу (нормализованный адрес + этаж + площадь + комнатность).
    - Удаление фейковых объявлений (слишком дешевые или с битыми данными).

### DQ проверки:
- Контроль объема удаленных данных.
- Мониторинг аномалий (слишком маленькая или большая площадь, цена).
- Количество уникальных районов или округов (слишком много может указывать на проблемы с парсингом адресов).

Запускается по датасету `RAW_DATASET_SALES_FLATS` и сам обновляет `SILVER_DATASET_SALES_FLATS`.
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": on_failure_callback,
}


def get_and_transform_raw_data_to_silver_s3(**context) -> dict[str, int]:
    """Очистка, дедубликация данных из слоя raw в silver .parquet и сохранение в S3"""
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    base_path = f"sales/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}"

    raw_s3_key = f"s3://{LAYER_SOURCE}/{base_path}/flats.jsonl"
    silver_s3_key = f"s3://{LAYER_TARGET}/{base_path}/flats.parquet"

    transform_raw_to_silver_query = load_sql(
        "transform_raw_to_silver.sql",
        raw_s3_key=raw_s3_key,
        silver_s3_key=silver_s3_key,
    )

    con = duckdb.connect()
    connect_duckdb_to_s3(con, "s3_conn")
    try:
        logging.info(f"💻 Выполняю трансформацию: {raw_s3_key}")
        con.execute(transform_raw_to_silver_query)
    finally:
        con.close()

    logging.info(f"✅ Файл успешно сохранен: {silver_s3_key}")

    return {  # автопуш ключей в xcoms
        "raw_s3_key": raw_s3_key,
        "silver_s3_key": silver_s3_key,
    }


def check_silver_data_quality(**context):
    """Проверка качества данных в silver слое после трансформации"""
    # вытаскиваем словарик ключей из xcoms
    keys = context["ti"].xcom_pull(task_ids="transform_to_silver")
    raw_s3_key = keys["raw_s3_key"]
    silver_s3_key = keys["silver_s3_key"]

    silver_dq_query = load_sql(
        "silver_dq.sql",
        silver_s3_key=silver_s3_key,
    )

    con = duckdb.connect()
    connect_duckdb_to_s3(con, "s3_conn")

    try:
        logging.info(f"💻 Выполняю проверку данных: {silver_s3_key}")
        dq_stats: tuple[int, int, float, float, int, int] = con.execute(silver_dq_query).fetchone()

        raw_total_rows: int = con.execute(f"SELECT count(*) FROM read_json_auto('{raw_s3_key}')").fetchone()[0]
    finally:
        con.close()

    silver_total_rows, districts, okrugs, min_area, max_area, min_price = dq_stats
    diff: int = raw_total_rows - silver_total_rows  # сколько строк удалилось в процессе трансформации
    percent_removed: float = (diff / raw_total_rows) * 100
    # проверки
    if silver_total_rows == 0:
        raise AirflowFailException("Файл пустой!")

    if percent_removed > 25:
        logging.error(f"❌ Удалено {percent_removed:.2f}% данных после трансформации.")
        raise AirflowFailException("Слишком много данных удалено!")

    if okrugs > 12:
        logging.error(f"❌ Слишком много уникальных округов - {okrugs}")
        raise AirflowFailException("Округов больше 12!")

    if districts > 127:
        logging.warning(f"⚠️ Много уникальных районов - {districts}")

    if min_area < 5:
        logging.error(f"❌ Слишком маленькая площадь: {min_area} м²")
        raise AirflowFailException("Маленькая площадь!")

    if max_area > 1500:
        logging.warning(f"⚠️ Подозрительно большая площадь: {max_area} м²")

    if min_price < 1_000_000:
        logging.error(f"❌ Слишком маленькая цена: {min_price} руб.")
        raise AirflowFailException()

    logging.info("✅ Проверка пройдена")
    logging.info(f"🧹 Удалено дублей и мусора: {diff} строк ({percent_removed:.2f}%).")

    return {"raw_count": raw_total_rows, "silver_count": silver_total_rows, "removed": diff}


with DAG(
    dag_id=DAG_ID,
    schedule=[RAW_DATASET_SALES_FLATS],  # как только обновится датасет raw запустится этот DAG
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["s3", "silver"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,  # будет отображаться в UI Airflow при открытии дага
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    transform_to_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=get_and_transform_raw_data_to_silver_s3,
    )

    check_data_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_silver_data_quality,
        on_success_callback=on_success_callback,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[SILVER_DATASET_SALES_FLATS],
    )

    start >> transform_to_silver >> check_data_quality >> end
