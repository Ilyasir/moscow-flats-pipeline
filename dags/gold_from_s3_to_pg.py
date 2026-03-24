import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.datasets import GOLD_DATASET_HISTORY, SILVER_DATASET_SALES_FLATS
from utils.duckdb import connect_duckdb_to_pg, connect_duckdb_to_s3, load_sql
from utils.telegram import on_failure_callback

OWNER = "ilyas"
DAG_ID = "gold_from_s3_to_pg"

LAYER_SOURCE = "silver"
LAYER_TARGET = "gold"

SHORT_DESCRIPTION = "Загрузка данных из S3 в Postgres DWH с поддержкой версионности цен по SCD2."

LONG_DESCRIPTION = """
## DAG: Gold Layer Ingestion (Postgres)
Этот DAG завершает основной процесс обработки данных, перенося их S3
в реляционную бд **PostgreSQL** для последующих витрин и аналитики
Сохраняет историю изменений цен на квартиры с помощью **SCD2**.

### Таски:
1. **load_from_s3_to_pg_stage**: 
    - Использует **DuckDB** как движок для передачи данных.
    - Через расширение `postgres` и с помощью `ATTACH` подключается напрямую к базе.
    - Очищает `stage_flats` и копирует туда данные из Parquet файла из S3.
    - Выполняет приведение типов ENUM: `okrug_name`, `transport_type`.
2. **merge_from_stage_to_history**:
    - Выполняет SQL-скрипт обновления истории изменений цен в таблице `history_flats`.
    - Реализует логику **SCD2**.

### Логика SCD2 (Бизнес-ключ):
Отслеживаем изменения поля `price` для уникального объекта (`flat_hash`).
Если цена изменилась:
- Поле `effective_to` у текущей записи закрывается датой парсинга.
- Поле `is_active` становится `FALSE`.
- Вставляется новая запись с новой ценой, `is_active = TRUE` и `effective_from = parsed_at`.

Запускается по датасету `SILVER_DATASET_SALES_FLATS` и обновляет `GOLD_DATASET_HISTORY`.
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": on_failure_callback,
}


def load_silver_data_from_s3_to_pg(**context) -> None:
    """Копипаст данных из S3 в stage таблицу postgres, с помощью duckdb"""
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    silver_s3_key = (
        f"s3://{LAYER_SOURCE}/sales/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.parquet"
    )
    # подключаемся к duckdb и настраиваем доступ к S3 и postgres
    con = duckdb.connect()
    connect_duckdb_to_s3(con, "s3_conn")
    connect_duckdb_to_pg(con, "pg_conn")

    try:
        logging.info(f"💻 Загружаю данные из {silver_s3_key} в stage таблицу")

        con.execute(load_sql("silver_to_stage_dwh.sql", silver_s3_key=silver_s3_key))
    finally:
        try:
            con.execute("DETACH flats_db;")  # отсоединеяем duckdb от postgres
        except Exception as e:
            logging.warning(f"⚠️ соединение уже разорвано: {e}")
        con.close()
    logging.info("✅ Успешно загружено в stage таблицу")


def check_merge_data(**context):
    """Проверяем, что после мержа количество активных записей в истории равно количеству записей в stage"""
    metrics = context["ti"].xcom_pull(task_ids="merge_from_stage_to_history")
    if not metrics:
        raise AirflowFailException()

    total_in_stage, active_after_merge, total_in_history = metrics[0]
    logging.info(f"📊 Stage: {total_in_stage}, Active: {active_after_merge}, Total: {total_in_history}")

    if total_in_stage != active_after_merge:
        raise AirflowFailException(f"Stage ({total_in_stage}) != Активные в history ({active_after_merge})")


with DAG(
    dag_id=DAG_ID,
    schedule=[SILVER_DATASET_SALES_FLATS],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["pg", "gold"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    load_from_s3_to_pg_stage = PythonOperator(
        task_id="load_from_s3_to_pg_stage", python_callable=load_silver_data_from_s3_to_pg
    )
    # выполняем merge из stage в историю с помощью SQL
    merge_from_stage_to_history = SQLExecuteQueryOperator(
        task_id="merge_from_stage_to_history",
        conn_id="pg_conn",
        autocommit=False,
        sql=load_sql("stage_to_history_scd2.sql"),
        show_return_value_in_logs=True,  # для отладки
    )

    check_data_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_merge_data,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[GOLD_DATASET_HISTORY],
    )

    start >> load_from_s3_to_pg_stage >> merge_from_stage_to_history >> check_data_quality >> end
