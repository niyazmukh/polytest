from __future__ import annotations

import json
from http.client import HTTPSConnection, HTTPResponse
from typing import Any, Dict, Optional


class HttpStatusError(RuntimeError):
    def __init__(self, status: int, body: str, method: str, path: str) -> None:
        super().__init__(f"http_error status={status} method={method} path={path} body={body[:400]}")
        self.status = int(status)
        self.body = body
        self.method = method
        self.path = path


class JsonHttpClient:
    """Small keep-alive JSON client for one host."""

    def __init__(self, host: str, timeout_seconds: float, user_agent: str) -> None:
        self.host = host
        self.timeout_seconds = float(timeout_seconds)
        self.user_agent = user_agent
        self._conn: Optional[HTTPSConnection] = None
        # Pre-built header dicts — avoid re-creating on every request.
        self._get_headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": user_agent,
            "Connection": "keep-alive",
        }
        self._post_headers: Dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": user_agent,
            "Connection": "keep-alive",
        }

    def _connect(self) -> HTTPSConnection:
        if self._conn is None:
            self._conn = HTTPSConnection(self.host, timeout=self.timeout_seconds)
        return self._conn

    def _reset(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @staticmethod
    def _read_json(response: HTTPResponse) -> Any:
        raw = response.read()
        text = raw.decode("utf-8")
        if not text:
            return None
        return json.loads(text)

    def get_json(self, path: str, headers: Optional[Dict[str, str]] = None) -> Any:
        req_headers = {**self._get_headers, **headers} if headers else self._get_headers
        last_error: Optional[Exception] = None
        for attempt in range(2):
            try:
                conn = self._connect()
                conn.request("GET", path, headers=req_headers)
                resp = conn.getresponse()
                if resp.status >= 400:
                    body = resp.read().decode("utf-8", errors="replace")
                    raise HttpStatusError(status=resp.status, body=body, method="GET", path=path)
                return self._read_json(resp)
            except HttpStatusError as exc:
                self._reset()
                if exc.status >= 500 and attempt < 1:
                    last_error = exc
                    continue
                raise
            except Exception as exc:
                self._reset()
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        raise RuntimeError("get_json_failed_after_retry")

    def post_json(self, path: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Any:
        req_headers = {**self._post_headers, **headers} if headers else self._post_headers
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        last_error: Optional[Exception] = None
        for attempt in range(2):
            try:
                conn = self._connect()
                conn.request("POST", path, body=body, headers=req_headers)
                resp = conn.getresponse()
                if resp.status >= 400:
                    text = resp.read().decode("utf-8", errors="replace")
                    raise HttpStatusError(status=resp.status, body=text, method="POST", path=path)
                return self._read_json(resp)
            except HttpStatusError as exc:
                self._reset()
                if exc.status >= 500 and attempt < 1:
                    last_error = exc
                    continue
                raise
            except Exception as exc:
                self._reset()
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        raise RuntimeError("post_json_failed_after_retry")

    def close(self) -> None:
        self._reset()
