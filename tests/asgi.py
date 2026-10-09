"""A minimal in-process ASGI client.

Starlette's TestClient depends on httpx, which pulls in certifi (MPL-2.0).
The admin tests only need to send a request and read the response, which
takes a few dozen lines of the ASGI protocol.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

from starlette.types import ASGIApp, Message


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes = b""

    @property
    def text(self) -> str:
        return self.body.decode()

    def json(self) -> Any:
        return json.loads(self.body)


@dataclass
class Client:
    app: ASGIApp
    headers: dict[str, str] = field(default_factory=dict)

    def with_basic_auth(self, username: str, password: str) -> Client:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        return Client(self.app, {**self.headers, "authorization": f"Basic {token}"})

    async def get(self, path: str, params: Mapping[str, object] | None = None) -> Response:
        return await self.request("GET", path, params=params)

    async def post(
        self,
        path: str,
        params: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        return await self.request("POST", path, params=params, headers=headers)

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
    ) -> Response:
        all_headers = {"host": "testserver", **self.headers, **(headers or {})}
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "root_path": "",
            "query_string": urlencode(params or {}).encode(),
            "headers": [(k.lower().encode(), v.encode()) for k, v in all_headers.items()],
            "client": ("127.0.0.1", 50000),
            "server": ("testserver", 80),
        }
        sent = False
        response = Response(status=0, headers={})

        async def receive() -> Message:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message: Message) -> None:
            if message["type"] == "http.response.start":
                response.status = message["status"]
                response.headers = {k.decode(): v.decode() for k, v in message["headers"]}
            elif message["type"] == "http.response.body":
                response.body += message.get("body", b"")

        await self.app(scope, receive, send)
        return response
