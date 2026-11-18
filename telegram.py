
"""Telegram 通知模块：发送 Sequoia-X 选股结果。"""

import os
import requests
from datetime import date


class TelegramNotifier:
    def __init__(self, settings=None):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not self.token or not self.chat_id:
            raise ValueError("缺少 Telegram Bot Token 或 Chat ID")

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
    ) -> None:
        today = date.today().strftime("%Y-%m-%d")

        lines = [
            "📈 Sequoia-X 选股播报",
            f"日期：{today}",
            f"策略：{strategy_name}",
            f"选股数量：{len(symbols)}",
            "",
            "选股列表：",
        ]

        lines.extend(f"• {code}" for code in symbols)

        message = "\n".join(lines)

        # Telegram 单条文本消息限制为 4096 字符
        chunks = []
        current = ""

        for line in message.split("\n"):
            if len(current) + len(line) + 1 > 3900:
                chunks.append(current)
                current = line
            else:
                current += ("\n" if current else "") + line

        if current:
            chunks.append(current)

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        for chunk in chunks:
            response = requests.post(
                url,
                json={
                    "chat_id": self.chat_id,
                    "text": chunk,
                },
                timeout=20,
            )
            response.raise_for_status()

            result = response.json()
            if not result.get("ok"):
                raise RuntimeError(f"Telegram 推送失败：{result}")
