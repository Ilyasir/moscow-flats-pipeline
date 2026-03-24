import json
import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.datasets import GOLD_DATASET_HISTORY
from utils.duckdb import connect_duckdb_to_pg, connect_duckdb_to_s3, load_sql
from utils.telegram import on_failure_callback, on_success_callback

OWNER = "ilyas"
DAG_ID = "ml_train_price_model"

SHORT_DESCRIPTION = "Пайплайн обучения catboost модели для оценки стоимости квадратного метра"

LONG_DESCRIPTION = """
### ML Training Pipeline

#### Таски:
**Prepare Training Dataset**:
    - Использует duckdb.
    - Извлекает данные из DWH `gold.history_flats`.
    - Использует оконные функции для дедубликации и выбора актуальных цен квартир (даже снятых с публикации).
    - Собирает и создает признаки.
    - Сохраняет датасет в S3 в формате parquet.
**Train Model**:
    - Запускает Docker-контейнер с CatBoost.
    - Обучает регрессию на скачанном из S3 датасете.
    - Загружает готовую модель (`.cbm`) обратно в S3.
    - Пишет логи и метрики в Airflow.
"""

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def get_ml_dataset_from_pg_to_s3(**context):
    """Берет данные из gold слоя и подготавливает датасет с новыми признаками для обучения, и в S3 грузит"""
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    dataset_s3_key = f"s3://ml-data/datasets/dataset_{dt.format('YYYY-MM-DD')}.parquet"
    model_s3_key = f"s3://ml-data/models/model_{dt.format('YYYY-MM-DD')}.cbm"

    query_prepare_dataset = load_sql(
        "prepare_dataset.sql",
        dataset_s3_key=dataset_s3_key,
    )

    con = duckdb.connect()
    connect_duckdb_to_s3(con, "s3_conn")
    connect_duckdb_to_pg(con, "pg_conn")
    try:
        logging.info(f"💻 Начинаю подготовку признаков и загрузку датасета в {dataset_s3_key}")
        con.execute(query_prepare_dataset)

    finally:
        try:
            con.execute("DETACH flats_db;")
        except Exception as e:
            logging.warning(f"⚠️ соединение уже разорвано: {e}")
        con.close()
    logging.info(f"✅ Датасет загружен в {dataset_s3_key}")

    return {
        "dataset_s3_key": dataset_s3_key,
        "model_s3_key": model_s3_key,
    }


def check_ml_metrics(**context):
    metrics = json.loads(context["ti"].xcom_pull(task_ids="train_model").strip().split("\n")[-1])

    mae = metrics.get("mae")
    mape = metrics.get("mape")
    rows = metrics.get("rows_trained")
    logging.info(f"📊 MAE: {mae}, MAPE: {mape}, Rows: {rows}")

    if mae > 150_000:
        logging.error(f"❌ MAE слишком высокое: {mae}")
        raise AirflowFailException()

    if float(mape.strip("%")) > 20.0:
        logging.error(f"❌ MAPE слишком высокое: {mape}")
        raise AirflowFailException("MAPE выше 20%")


with DAG(
    dag_id=DAG_ID,
    schedule=[GOLD_DATASET_HISTORY],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "gold", "s3"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    prepare_training_dataset = PythonOperator(
        task_id="prepare_training_dataset",
        python_callable=get_ml_dataset_from_pg_to_s3,
    )

    # обучение модели в контейнере с catboost
    train_model = DockerOperator(
        task_id="train_model",
        image="catboost_train:latest",
        container_name="catboost_train_container",
        api_version="auto",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="data_network",
        mount_tmp_dir=False,
        tty=True,
        mem_limit="1g",  # для catboost 1 хватит
        environment={
            # для доступа к S3 из контейнера
            "S3_ACCESS_KEY": "{{ conn.s3_conn.login }}",
            "S3_SECRET_KEY": "{{ conn.s3_conn.password }}",
            "S3_ENDPOINT_URL": "{{ conn.s3_conn.extra_dejson.endpoint_url }}",
            "S3_REGION_NAME": "{{ conn.s3_conn.extra_dejson.region_name }}",
            "S3_BUCKET_NAME": "ml-data",
            # берем ключи из xcoms прошлой таски, передаем в конейнер, потом он скачает датасет и загрузит модель
            "DATASET_S3_KEY": "{{ task_instance.xcom_pull(task_ids='prepare_training_dataset')['dataset_s3_key'] }}",
            "MODEL_S3_KEY": "{{ task_instance.xcom_pull(task_ids='prepare_training_dataset')['model_s3_key'] }}",
        },
        on_success_callback=on_success_callback,
    )

    check_metrics = PythonOperator(
        task_id="check_metrics",
        python_callable=check_ml_metrics,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> prepare_training_dataset >> train_model >> check_metrics >> end
