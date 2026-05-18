from __future__ import annotations

import json
from typing import Any, Dict, Optional

import httpx

from .config import Settings, get_settings


class WorkspaceMCPError(RuntimeError):
    pass


def _parse_sse_payload(text: str) -> Optional[Dict[str, Any]]:
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if not payload or payload == "[DONE]":
            continue
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _extract_result_text(result: Any) -> str:
    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        content = result.get("content")
        if isinstance(content, list):
            parts = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
            if parts:
                return "\n".join(parts)
        for key in ("text", "output", "result"):
            if isinstance(result.get(key), str):
                return result[key]
    return ""


class WorkspaceMCPClient:
    def __init__(
        self,
        *,
        url: Optional[str] = None,
        bearer_token: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
        retries: Optional[int] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        settings = settings or get_settings()
        self.url = (url or settings.google_workspace_mcp_url or "").strip()
        if not self.url:
            raise WorkspaceMCPError("GOOGLE_WORKSPACE_MCP_URL is not configured")
        self.bearer_token = bearer_token or settings.google_workspace_mcp_bearer_token
        self.timeout_seconds = float(timeout_seconds if timeout_seconds is not None else settings.google_workspace_mcp_timeout_seconds)
        self.retries = max(0, int(retries if retries is not None else settings.google_workspace_mcp_retries))
        self._request_id = 0
        self._session_id: Optional[str] = None
        self._initialized = False

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        return headers

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _post_jsonrpc(self, method: str, params: Optional[Dict[str, Any]] = None, *, expect_response: bool = True) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if expect_response:
            payload["id"] = self._next_id()
        if params is not None:
            payload["params"] = params

        last_error: Optional[Exception] = None
        for _attempt in range(self.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    response = await client.post(self.url, headers=self._headers(), json=payload)
                if "mcp-session-id" in response.headers:
                    self._session_id = response.headers.get("mcp-session-id")
                response.raise_for_status()
                if not expect_response:
                    return {}
                ctype = response.headers.get("content-type", "")
                if "text/event-stream" in ctype:
                    parsed = _parse_sse_payload(response.text)
                else:
                    parsed = response.json()
                if not isinstance(parsed, dict):
                    raise WorkspaceMCPError("Invalid MCP response")
                if parsed.get("error"):
                    raise WorkspaceMCPError(str(parsed["error"]))
                return parsed
            except Exception as exc:
                last_error = exc
        raise WorkspaceMCPError(str(last_error))

    async def initialize(self) -> None:
        if self._initialized:
            return
        await self._post_jsonrpc(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "synapse", "version": "0.1.0"},
            },
        )
        try:
            await self._post_jsonrpc("notifications/initialized", expect_response=False)
        except WorkspaceMCPError:
            # Some stateless deployments accept direct tool calls after initialize.
            pass
        self._initialized = True

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Any:
        await self.initialize()
        response = await self._post_jsonrpc("tools/call", {"name": name, "arguments": arguments})
        return response.get("result")

    async def call_tool_text(self, name: str, arguments: Dict[str, Any]) -> str:
        return _extract_result_text(await self.call_tool(name, arguments))

