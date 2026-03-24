import logging
import os

import boto3
from botocore.client import Config

ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENDPOINT = os.getenv("S3_ENDPOINT_URL")
REGION_NAME = os.getenv("S3_REGION_NAME", "ru-central1")

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION_NAME,
    config=Config(signature_version="s3v4"),
)


def download_file_from_s3(bucket_name: str, local_file_path: str, object_key: str) -> bool:
    """Скачивает файл из S3 по ключу и сохраняет его локально."""
    try:
        s3_client.download_file(Bucket=bucket_name, Filename=local_file_path, Key=object_key)
        logging.info(f"✅ Скачано из S3: {object_key} -> {local_file_path}")
        return True
    except Exception as e:
        logging.error(f"❌ Ошибка скачивания {object_key}: {e}")
        return False


def upload_file_to_s3(local_file_path: str, bucket_name: str, object_key: str) -> bool:
    """Загружает локальный файл в S3"""
    try:
        s3_client.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=object_key)
        logging.info(f"✅ Загружено в S3: {local_file_path} -> {object_key}")
        return True
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки {local_file_path} в S3: {e}")
        return False
