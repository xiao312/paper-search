from __future__ import annotations

import logging
import os
from typing import Optional

from papersearch.adapters.feishu.webhook_client import FeishuWebhookClient

logger = logging.getLogger(__name__)


class FeishuNotifier:
    def __init__(self, client: FeishuWebhookClient):
        self.client = client

    @classmethod
    def from_env(cls) -> Optional["FeishuNotifier"]:
        enabled = os.getenv("FEISHU_NOTIFY_ENABLED", "false").lower() == "true"
        webhook = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
        if not enabled or not webhook:
            return None
        secret = os.getenv("FEISHU_SIGNING_SECRET", "").strip() or None
        client = FeishuWebhookClient(webhook_url=webhook, signing_secret=secret)
        return cls(client)

    def notify_search_completed(self, search_id: str, query: str, relevant_found: int, completeness: float) -> None:
        msg = (
            f"✅ Search completed\n"
            f"search_id: {search_id}\n"
            f"query: {query[:120]}\n"
            f"relevant_found: {relevant_found}\n"
            f"completeness: {completeness:.2f}"
        )
        try:
            self.client.send_text(msg)
        except Exception as e:
            # phase-1: notifier failures should not fail core flow
            logger.warning("feishu notification failed: %s", e)
