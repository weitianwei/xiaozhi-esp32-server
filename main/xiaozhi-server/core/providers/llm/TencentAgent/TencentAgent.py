import asyncio
import json
import random
import string
import threading
import time
import uuid
from queue import Empty, Queue

try:
    from Crypto.Cipher import AES
except Exception:
    AES = None

import requests
import websockets
from config.logger import setup_logging
from core.providers.llm.base import LLMProviderBase
from core.providers.llm.system_prompt import get_system_prompt_for_function

TAG = __name__
logger = setup_logging()


class LLMProvider(LLMProviderBase):
    """Tencent Cloud LKE/QBot websocket agent adapter."""

    def __init__(self, config):
        self.websocket_url = config.get(
            "websocket_url",
            "wss://wss.lke.cloud.tencent.com/v1/qbot/chat/conn/?EIO=4&transport=websocket",
        )
        self.token = config.get("token", "")
        self.token_url = config.get("token_url", "")
        self.decrypt_token = str(config.get("decrypt_token", False)).lower() in ("1", "true", "yes", "on")
        self.encryption_key = config.get("encryption_key", "")
        self.agent_id = str(config.get("agent_id", ""))
        self.source = str(config.get("source", ""))
        self.timeout = int(config.get("timeout", 120))
        self.session_map = {}

    def _random_request_id(self, length=10):
        chars = string.ascii_letters + string.digits
        return "".join(random.choice(chars) for _ in range(length)) + "-" + str(random.randint(0, 9999999999))

    def _decrypt_token_if_needed(self, token):
        if not self.decrypt_token or not token or ":" not in token:
            return token
        if AES is None:
            raise RuntimeError("TencentAgent decrypt_token requires pycryptodome: pip install pycryptodome")
        if not self.encryption_key:
            raise ValueError("TencentAgent encryption_key is required when decrypt_token is enabled")

        iv_hex, cipher_hex = token.split(":", 1)
        key = self.encryption_key.encode("utf-8")
        iv = bytes.fromhex(iv_hex)
        ciphertext = bytes.fromhex(cipher_hex)
        cipher = AES.new(key, AES.MODE_CBC, iv)
        plaintext = cipher.decrypt(ciphertext)
        pad_len = plaintext[-1]
        if pad_len < 1 or pad_len > AES.block_size:
            raise ValueError("TencentAgent token decrypt failed: invalid PKCS7 padding")
        return plaintext[:-pad_len].decode("utf-8")

    def _get_token(self):
        if self.token:
            return self._decrypt_token_if_needed(self.token)
        if not self.token_url:
            raise ValueError("TencentAgent requires token or token_url")

        params = {}
        if self.agent_id:
            params["id"] = self.agent_id
        if self.source:
            params["source"] = self.source

        resp = requests.get(self.token_url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        request_info = data.get("requestInfo") or {}
        if request_info:
            logger.bind(tag=TAG).info(
                "TencentAgent token resolved agent_id=%s name=%s"
                % (request_info.get("agentId"), request_info.get("name"))
            )

        token = (
            data.get("apiResponse", {}).get("Token")
            or data.get("Token")
            or data.get("token")
            or data.get("data", {}).get("token")
            or data.get("data", {}).get("Token")
        )
        if not token:
            raise ValueError("TencentAgent token_url response does not contain token")
        return self._decrypt_token_if_needed(token)

    def _last_user_content(self, dialogue):
        last_msg = next(m for m in reversed(dialogue) if m.get("role") == "user")
        return str(last_msg.get("content", ""))

    def _build_function_dialogue(self, dialogue, functions):
        prepared = [dict(message) for message in dialogue]

        if functions:
            function_str = json.dumps(functions, ensure_ascii=False)
            function_prompt = get_system_prompt_for_function(function_str)
            for index in range(len(prepared) - 1, -1, -1):
                if prepared[index].get("role") == "user":
                    prepared[index]["content"] = function_prompt + str(
                        prepared[index].get("content", "")
                    )
                    break

        if prepared and prepared[-1].get("role") == "tool":
            tool_result = str(prepared[-1].get("content", ""))
            tool_prompt = (
                "\n\ntool call result:\n"
                + tool_result
                + "\n\nUse the tool result above to continue the user's request. "
                "If another tool is needed, call exactly one tool in <tool_call> JSON format. "
                "Otherwise answer the user directly."
            )
            for index in range(len(prepared) - 1, -1, -1):
                if prepared[index].get("role") == "user":
                    prepared[index]["content"] = (
                        str(prepared[index].get("content", "")) + tool_prompt
                    )
                    break

        return prepared

    def _extract_reply_text(self, content):
        text = str(content or "")
        if not text:
            return ""
        try:
            data = json.loads(text)
        except Exception:
            return text
        if isinstance(data, dict):
            for key in ("dyyj_001_answer", "answer", "content", "text"):
                value = data.get(key)
                if value:
                    return str(value)
        return text

    async def _run_chat(self, session_id, content, out_queue):
        token = self._get_token()
        session_key = session_id or "default"
        lke_session_id = self.session_map.setdefault(session_key, str(uuid.uuid4()))
        request_id = self._random_request_id()
        full_text = ""
        last_text = ""

        payload = {
            "request_id": request_id,
            "session_id": lke_session_id,
            "is_msg_status": True,
            "content": content,
            "realContent": content,
            "incremental": True,
        }

        async with websockets.connect(self.websocket_url, ping_interval=None, close_timeout=5) as ws:
            await ws.send("40" + json.dumps({"token": token}, ensure_ascii=False))

            authenticated = False
            deadline = time.time() + self.timeout
            while time.time() < deadline:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(1, int(deadline - time.time())))
                if raw == "2":
                    await ws.send("3")
                    continue
                if isinstance(raw, str) and raw.startswith("40"):
                    authenticated = True
                    break

            if not authenticated:
                raise TimeoutError("TencentAgent websocket authentication timeout")

            await ws.send("42" + json.dumps(["send", {"payload": payload}], ensure_ascii=False))

            while time.time() < deadline:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(1, int(deadline - time.time())))
                if raw == "2":
                    await ws.send("3")
                    continue
                if not isinstance(raw, str) or not raw.startswith("42"):
                    continue

                try:
                    json_start = raw.find("[")
                    if json_start < 0:
                        continue
                    event_name, body = json.loads(raw[json_start:])
                except Exception:
                    logger.bind(tag=TAG).warning(f"TencentAgent invalid websocket message: {raw[:200]}")
                    continue

                payload = body.get("payload", {}) if isinstance(body, dict) else {}
                if event_name == "thought" or event_name == "token_stat":
                    continue
                if event_name != "reply":
                    continue
                if payload.get("is_from_self"):
                    continue
                if payload.get("request_id") and payload.get("request_id") != request_id:
                    continue
                if payload.get("can_rating") is False and not payload.get("is_final"):
                    continue

                text = self._extract_reply_text(payload.get("content") or "")
                if text:
                    if text.startswith(last_text):
                        delta = text[len(last_text):]
                    elif len(text) > len(full_text) and text.startswith(full_text):
                        delta = text[len(full_text):]
                    else:
                        delta = text
                    if delta:
                        out_queue.put(delta)
                    last_text = text
                    full_text += delta

                if payload.get("is_final") or payload.get("can_rating") is True:
                    break

    def response(self, session_id, dialogue, **kwargs):
        content = self._last_user_content(dialogue)
        out_queue = Queue()

        def runner():
            try:
                asyncio.run(self._run_chat(session_id, content, out_queue))
            except Exception as e:
                logger.bind(tag=TAG).error(f"TencentAgent call failed: {e}")
                out_queue.put(f"【腾讯智能体调用失败：{e}】")
            finally:
                out_queue.put(None)

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()

        while True:
            try:
                item = out_queue.get(timeout=0.5)
            except Empty:
                if not thread.is_alive():
                    break
                continue
            if item is None:
                break
            yield item

    def response_with_functions(self, session_id, dialogue, functions=None, **kwargs):
        dialogue = self._build_function_dialogue(dialogue, functions)
        for token in self.response(session_id, dialogue, **kwargs):
            yield token, None
