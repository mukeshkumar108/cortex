import asyncio
import pytest

from src.openrouter_client import get_llm_client


@pytest.fixture(autouse=True)
def _stub_llm_calls(monkeypatch):
    llm_client = get_llm_client()

    async def _stub_call_llm(*_args, **_kwargs):
        return "summary"

    monkeypatch.setattr(llm_client, "_call_llm", _stub_call_llm, raising=True)
