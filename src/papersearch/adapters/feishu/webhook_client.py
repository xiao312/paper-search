from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.request
from typing import Optional


class FeishuWebhookClient:
    def __init__(self, webhook_url: str, signing_secret: Optional[str] = None, timeout: int = 8):
        self.webhook_url = webhook_url
        self.signing_secret = signing_secret
        self.timeout = timeout

    def send_text(self, text: str) -> None:
        payload = {"msg_type": "text", "content": {"text": text}}
        self._post(payload)

    def _post(self, payload: dict) -> None:
        if self.signing_secret:
            ts = str(int(time.time()))
            sign = self._sign(ts)
            payload["timestamp"] = ts
            payload["sign"] = sign

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            _ = resp.read()

    def _sign(self, timestamp: str) -> str:
        to_sign = f"{timestamp}\n{self.signing_secret}".encode("utf-8")
        digest = hmac.new(to_sign, b"", digestmod=hashlib.sha256).digest()
        return __import__("base64").b64encode(digest).decode("utf-8")
