import logging
from utils.tg_hook import TelegramAlertHook

logger = logging.getLogger(__name__)


def on_failure_tg(context):
   
    try:
        dag_id = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        execution_date = context["execution_date"]
        exception = context.get("exception")

        message = (
            f"❌ Ошибка в DAG!\n"
            f"DAG: {dag_id}\n"
            f"Task: {task_id}\n"
            f"Время: {execution_date}\n"
            f"Ошибка: {exception}"
        )

        TelegramAlertHook().send_message(message)

    except Exception as e:
        logger.error("Не удалось отправить сообщение в Telegram: %s", e)