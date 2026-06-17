from airflow.models import Variable
from airflow.hooks.base import BaseHook
import logging
import requests

logger = logging.getLogger(__name__)


class TelegramAlertHook(BaseHook):
    def __init__(self, bot_token: str = Variable.get("bot_token"), chat_id: str = Variable.get("tg_chat_id")):
        super().__init__()
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send_message(self, text: str):
        if not self.bot_token or not self.chat_id:
            logger.info("Не задан токен или чат айди")
            return
        
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        res = requests.post(url, data={"chat_id": self.chat_id, "text": text})

        res.raise_for_status()

        print(res.status_code)
