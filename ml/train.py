import json
import logging
import os

import pandas as pd
from catboost import CatBoostRegressor
from s3_client import download_file_from_s3, upload_file_to_s3
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
from sklearn.model_selection import train_test_split

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# читаем ключи и название бакета из переменных окружения
BUCKET = os.getenv("S3_BUCKET_NAME", "ml-data")
DATASET_S3_KEY = os.getenv("DATASET_S3_KEY").replace("s3://ml-data/", "")  # убираем префикс
MODEL_S3_KEY = os.getenv("MODEL_S3_KEY").replace("s3://ml-data/", "")  # и бакет

LOCAL_DATA = "data.parquet"
LOCAL_MODEL = "model.cbm"


def train() -> dict:
    logging.info(f"📥 Скачиваю датасет из S3: {DATASET_S3_KEY}")
    download_file_from_s3(BUCKET, LOCAL_DATA, DATASET_S3_KEY)

    df = pd.read_parquet(LOCAL_DATA)
    logging.info(f"📊 Датасет загружен. Строк: {len(df)}")

    # подготовка признаков для модели
    cat_features = [
        "okrug",
        "district",
        "is_apartament",
        "is_studio",
        "is_new_moscow",
        "is_first_floor",
        "is_last_floor",
        "is_high_rise",
    ]

    # все в строки, чтобы catboost понимал
    for col in cat_features:
        df[col] = df[col].astype(str)

    X = df.drop(columns=["price_per_meter"])
    y = df["price_per_meter"]
    # разбиваем 80 на 20
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logging.info("🚀 Обучение Catboost...")
    model = CatBoostRegressor(
        iterations=4000,
        learning_rate=0.05,
        depth=8,
        l2_leaf_reg=3,
        loss_function="RMSE",
        eval_metric="MAPE",
        early_stopping_rounds=100,  # остановка раньше времени, если метрика не улучшается
        random_seed=42,
        verbose=200,  # выводить метрики каждые 200 итераций
    )

    model.fit(X_train, y_train, eval_set=(X_test, y_test), cat_features=cat_features)

    # мтерики на тесте
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    mape = mean_absolute_percentage_error(y_test, preds)

    logging.info("✅ Обучение завершено!")
    logging.info(f"📈 MAE: {round(mae)} руб. | MAPE: {round(mape * 100, 2)}%")
    # сохраняем локально и потом в S3
    model.save_model(LOCAL_MODEL)
    upload_file_to_s3(LOCAL_MODEL, BUCKET, MODEL_S3_KEY)

    metrics = {
        "mae": round(mae),
        "mape": f"{round(mape * 100, 2)}%",
        "rows_trained": len(df),
    }

    return metrics


if __name__ == "__main__":
    metrics = train()
    print(json.dumps(metrics))  # для того, чтобы в xcom потом запушить
