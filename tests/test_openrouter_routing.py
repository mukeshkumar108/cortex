from typing import List

import pytest

from src.openrouter_client import OpenRouterClient
from src.config import get_settings


class _FakeResponse:
    status_code = 200

    @staticmethod
    def json():
        return {"choices": [{"message": {"content": "ok"}}]}


class _FakeAsyncClient:
    def __init__(self, *args, **kwargs):
        self._captures = kwargs.get("captures")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, _url, headers=None, json=None):
        if self._captures is not None:
            self._captures.append(json)
        return _FakeResponse()


@pytest.mark.asyncio
async def test_openrouter_model_routing(monkeypatch):
    captures: List[dict] = []

    monkeypatch.setenv("OPENROUTER_MODEL_SUMMARY", "amazon/nova-micro")
    monkeypatch.setenv("OPENROUTER_MODEL_LOOPS", "xiaomi/mimo-v2-flash")
    monkeypatch.setenv("OPENROUTER_MODEL_SESSION_EPISODE", "xiaomi/mimo-v2-flash")
    monkeypatch.setenv("OPENROUTER_MODEL_FALLBACK", "mistral/ministral-3b")
    get_settings.cache_clear()

    def _client_factory(*_args, **_kwargs):
        return _FakeAsyncClient(captures=captures)

    monkeypatch.setattr("src.openrouter_client.httpx.AsyncClient", _client_factory)

    client = OpenRouterClient()

    await client._call_llm("hi", task="summary")
    await client._call_llm("hi", task="loops")
    await client._call_llm("hi", task="session_episode")
    await client._call_llm("hi", task="generic")

    models = [payload["model"] for payload in captures]
    assert models[0] == "amazon/nova-micro"
    assert models[1] == "xiaomi/mimo-v2-flash"
    assert models[2] == "xiaomi/mimo-v2-flash"
    assert models[3] == client.model
