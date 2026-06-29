import logging
from typing import Dict

logger = logging.getLogger(__name__)

class TelegramAlerts:
    """No-op Telegram alerts stub to disable Telegram integration while preserving calls.

    This class intentionally does nothing except light logging so existing code
    that imports and calls TelegramAlerts keeps working without sending messages.
    """
    def __init__(self, bot_token: str = "", chat_id: str = ""):
        self.bot_token = bot_token
        self.chat_id = chat_id
        logger.info("TelegramAlerts stub initialized (no-op)")

    def send_message(self, text: str):
        logger.info(f"Telegram stub send_message: {text}")

    def trade_alert(self, data: Dict):
        logger.info(f"Telegram stub trade_alert: {data}")

    def profit_alert(self, data: Dict):
        logger.info(f"Telegram stub profit_alert: {data}")

    def error_alert(self, text: str):
        logger.error(f"Telegram stub error_alert: {text}")

    def daily_summary(self, data: Dict):
        logger.info("Telegram stub daily_summary called")
