import json
import logging

from airflow.providers.telegram.hooks.telegram import TelegramHook


def send_telegram_message(context, status="error"):
    ti = context.get("task_instance")
    dag_id = ti.dag_id
    execution_date = context.get("logical_date").in_timezone("Europe/Moscow").strftime("%Y-%m-%d %H:%M:%S")
    # путь до логов упавшей таски
    log_url = ti.log_url

    if status == "success":
        message = f"<b>✅ DAG Success: {dag_id}</b>\n\n"
        message += f"📅 <b>Time:</b> {execution_date}\n\n"

        # тянем все XComs текущего запуска DAG
        xcoms = ti.xcom_pull(dag_id=dag_id, task_ids=None, include_prior_dates=False)

        if xcoms:
            message += "📋 <b>Statistics from XCom:</b>\n"
            # xcoms этих тасок отправляем
            important_tasks = ["check_data_quality", "train_model"]

            for t_id in important_tasks:
                # вытаскиваем инфу из xcom
                data = ti.xcom_pull(task_ids=t_id)

                if data:
                    if isinstance(data, str):  # если в xcom в формате строки
                        data = json.loads(data)  # превращаем строку в json
                    if isinstance(data, dict):
                        message += f"\n🔹 <i>Task: {t_id}</i>\n"
                        # форматируем для красоты
                        for key, value in data.items():
                            clean_key = key.replace("_", " ").capitalize()
                            val_str = f"<code>{value}</code>" if isinstance(value, (int, float)) else str(value)
                            message += f"  • {clean_key}: {val_str}\n"
        else:
            message += "<i>No statistics found in XCom.</i>"
    else:
        # алерт об ошибке
        message = (
            f"<b>❌ Task Failed!</b>\n\n"
            f"<b>DAG:</b> <code>{dag_id}</code>\n"
            f"<b>Task:</b> <code>{ti.task_id}</code>\n"
            f"<b>Time:</b> {execution_date}\n"
            f"<b>Logs:</b> <a href='{log_url}'>Open Airflow UI</a>"
            f"\n{log_url}"  # просто текстом выводим ссылку, если будет некликабельно
        )

    try:
        hook = TelegramHook(telegram_conn_id="telegram_conn")
        hook.send_message({"text": message, "parse_mode": "HTML", "disable_web_page_preview": True})
    except Exception as e:
        logging.error(f"Ошибка отправки в Telegram: {e}")


def on_failure_callback(context):
    send_telegram_message(context, status="error")


def on_success_callback(context):
    send_telegram_message(context, status="success")
