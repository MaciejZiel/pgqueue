"""The nginx files that ship with the demo, checked against the dashboard they protect."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
TERMUX = ROOT / "deploy" / "termux"
ADMIN = ROOT / "src" / "pgqueue" / "admin"
HEADERS = "include /data/data/com.termux/files/home/pgqueue/deploy/termux/nginx-headers.conf;"


def csp() -> dict[str, list[str]]:
    text = (TERMUX / "nginx-headers.conf").read_text()
    match = re.search(r'Content-Security-Policy "([^"]+)"', text)
    assert match
    directives = {}
    for part in match.group(1).split(";"):
        name, *values = part.split()
        directives[name] = values
    return directives


def test_the_policy_allows_only_the_dashboards_own_resources() -> None:
    policy = csp()
    for name in ("default-src", "script-src", "style-src", "connect-src"):
        assert policy[name] == ["'self'"], name
    assert policy["frame-ancestors"] == ["'none'"]
    assert policy["object-src"] == ["'none'"]
    assert not any("unsafe" in v for values in policy.values() for v in values)


def test_the_dashboard_needs_nothing_the_policy_forbids() -> None:
    html = (ADMIN / "templates" / "dashboard.html").read_text()
    script = (ADMIN / "static" / "dashboard.js").read_text()
    css = (ADMIN / "static" / "dashboard.css").read_text()

    assert not re.search(r"<script(?![^>]*\bsrc=)", html), "inline script"
    assert not re.search(r"<style", html), "inline style element"
    assert not re.search(r"\sstyle=", html), "inline style attribute"
    assert not re.search(r"\son[a-z]+=", html), "inline event handler"
    assert not re.search(r"\sstyle=|\bon[a-z]+=\"", script), "inline style or handler built in JS"
    assert not re.search(r"\bstyle:\s*[`\"']", script), "style attribute set from a string"
    assert not re.search(r"setAttribute\(\s*[\"']style", script), "style attribute set from JS"
    # Links to other sites are fine; anything the page loads or connects to is not.
    assert not re.search(r"<(?:script|img|link|iframe)[^>]+(?:src|href)=\"https?://", html)
    assert not re.search(r"(?:fetch|EventSource|import)\(\s*[\"'`]https?://", script)
    assert not re.search(r"@import|url\(\s*['\"]?https?:", css), "external stylesheet resource"
    assert not re.search(r"\beval\(|new Function\(", script)


@pytest.mark.parametrize("path", ["/metrics", "/docs", "/redoc", "/openapi.json"])
def test_nginx_answers_404_for_the_internal_endpoints(path: str) -> None:
    text = (TERMUX / "nginx-location.conf").read_text()
    match = re.search(r"location ~ (\^[^ ]+) \{[^}]*return 404", text)
    assert match
    assert re.match(match.group(1), "/pgqueue" + path)
    assert re.match(match.group(1), "/pgqueue" + path + "/")
    assert not re.match(match.group(1), "/pgqueue/api/live")


def test_every_location_repeats_the_security_headers() -> None:
    text = (TERMUX / "nginx-location.conf").read_text()
    blocks = re.findall(r"^location [^\n]*\{(.*?)^\}", text, re.S | re.M)
    assert len(blocks) >= 6
    for block in blocks:
        assert HEADERS in block
