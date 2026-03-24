import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.datasets import RAW_DATASET_SALES_FLATS
from utils.duckdb import connect_duckdb_to_s3, load_sql
from utils.telegram import on_failure_callback, on_success_callback

OWNER = "ilyas"
DAG_ID = "raw_from_parser_to_s3"

LAYER = "raw"

SHORT_DESCRIPTION = (
    "Сбор сырых данных о недвижимости через Docker-парсер и первичная проверка качества в S3, c помощью duckdb"
)

LONG_DESCRIPTION = """
## DAG: Raw Data Ingestion
Данный DAG является началом всего пайплайна.
Он отвечает за извлечение данных из внешнего источника и их сохранение в Data Lake.

### Основные таски:
1. **run_parser**: Запуск контейнера с парсером через `DockerOperator`. 
    - Использует **Playwright** внутри для обхода динамических элементов.
    - Контейнеру выделено 3GB RAM для нормальной работы **Playwright**.
    - Результаты сразу сохраняются в **S3 (Minio)** в формате `.jsonl`.
    - Партиционирование в стиле Hive: `year=YYYY/month=MM/day=DD/`.
2. **check_data_quality**: Валидация собранного файла с помощью **DuckDB**. 
    - Проверка на пустые строки.
    - Контроль уникальности объявлений по ID.
    - Проверка заполненности полей: цена, адрес, метро, описание.

### Особенности:
- **Расписание**: Ежедневно
- **Идемпотентность**: Ограничена, так как парсер берет текущий срез сайта, невозможно взять данные за конкретную дату.
Но запуск за конкретную дату перезаписывает файл в соответствующей папке S3.
После выполнения обновляет `RAW_DATASET_SALES_FLATS`.
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 1,
    "retry_delay": pendulum.duration(hours=1),
    "on_failure_callback": on_failure_callback,
}


def check_raw_data_quality(**context) -> dict[str, int]:
    """Проверка качества данных в S3 с помощью duckdb"""
    # Формируем путь к файлу в S3
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    raw_s3_key = f"s3://{LAYER}/sales/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.jsonl"

    con = duckdb.connect()
    connect_duckdb_to_s3(con, "s3_conn")
    try:
        logging.info(f"💻 Выполняю проверку данных: {raw_s3_key}")
        dq_stats: tuple[int, int, int, int, int, int] = con.execute(
            load_sql("raw_dq.sql", raw_s3_key=raw_s3_key)
        ).fetchone()

    finally:
        con.close()
    # распаковываем результаты из кортежа
    total_rows, unique_ids, valid_prices, valid_addresses, valid_metro, valid_description = dq_stats
    # если сток нет, сразу фейлим
    if total_rows == 0:
        raise AirflowFailException("Файл пустой!")
    # метрики качетсва
    unique_ids_rate: float = unique_ids / total_rows
    valid_prices_rate: float = valid_prices / total_rows
    valid_addresses_rate: float = valid_addresses / total_rows
    valid_metro_rate: float = valid_metro / total_rows
    valid_description_rate: float = valid_description / total_rows if total_rows > 0 else 0
    # проверки
    if unique_ids_rate < 0.90:
        logging.error(f"❌ Проверка не пройдена. Уникальных ID - {unique_ids_rate:.2%}")
        raise AirflowFailException("Слишком много дублирующихся ID!")

    if valid_prices_rate < 0.95:
        logging.error(f"❌ Проверка не пройдена. Процент заполненных цен - {valid_prices_rate:.2%}")
        raise AirflowFailException("Cлишком много пустых цен!")

    if valid_addresses_rate < 0.95:
        logging.error(f"❌ Проверка не пройдена. Процент заполненных адресов - {valid_addresses_rate:.2%}")
        raise AirflowFailException("Cлишком много пустых адресов!")

    logging.info(
        f"✅ Проверка пройдена. Всего строк: {total_rows}. "
        f"Процент заполненных адресов: {valid_addresses_rate:.2%}. "
        f"Процент заполненных метро: {valid_metro_rate:.2%}. "
        f"Процент заполненных описаний: {valid_description_rate:.2%}"
    )

    return {  # пуш в xcoms, чтобы в UI XCom видеть статистику
        "total_rows": total_rows,
        "unique_ids": unique_ids,
        "valid_prices": valid_prices,
        "valid_addresses": valid_addresses,
        "valid_metro": valid_metro,
        "valid_description": valid_description,
    }


with DAG(
    dag_id=DAG_ID,
    schedule="0 23 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    run_parser = DockerOperator(
        task_id="run_parser",
        image="flats-parser:2.0",  # образ с парсером, надо заранее билдить
        container_name="flats_parser_container",
        api_version="auto",
        auto_remove="force",  # удаляем контейнер в любом случае, логи все равно проброшены а аирфоу
        docker_url="unix://var/run/docker.sock",  # чтобы аирфлоу мог запускать контейнеры в докере
        network_mode="data_network",  # все сервисы в этой сети (парсер тоже), чтобы видеть в minio
        mount_tmp_dir=False,  # временная папка не нужна, так как из контейнера данные сразу идут в S3
        tty=True,  # логи контейнера будут видны в UI Airflow
        mem_limit="4g",  # ограничение по памяти для контейнера
        shm_size="1g",  # для хрома внутри контейнера, чтобы не было ошибок с памятью при парсинге
        # параметры доступа к S3 через переменные окружения, чтобы внутри контейнера можно было сохранять в S3
        environment={
            "S3_ACCESS_KEY": "{{ conn.s3_conn.login }}",
            "S3_SECRET_KEY": "{{ conn.s3_conn.password }}",
            "S3_ENDPOINT_URL": "{{ conn.s3_conn.extra_dejson.endpoint_url }}",
            "S3_REGION_NAME": "{{ conn.s3_conn.extra_dejson.region_name }}",
            "S3_BUCKET_NAME": LAYER,
            "TZ": "Europe/Moscow",
            "EXECUTION_DATE": "{{ data_interval_end.in_timezone('Europe/Moscow').format('YYYY-MM-DD') }}",
        },
    )

    check_data_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_raw_data_quality,
        on_success_callback=on_success_callback,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[RAW_DATASET_SALES_FLATS],  # триггерим датасет, чтобы запустить следующий DAG
    )

    start >> run_parser >> check_data_quality >> end
