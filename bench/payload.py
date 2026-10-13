"""Deterministic job payloads of a chosen serialized size.

The shape imitates a typical background job (a transactional e-mail or a webhook
delivery): a few identifiers, short strings and a free-text body. The body is made of
words drawn from a fixed vocabulary with a fixed seed, so every run, and both queue
systems, see the same bytes. Real text compresses somewhat; so does this.
"""

from __future__ import annotations

import json
import random
from typing import Any

SIZES: dict[str, int] = {"small": 256, "medium": 2048, "large": 16384}

_VOCAB = [
    "order",
    "invoice",
    "customer",
    "shipment",
    "warehouse",
    "confirmation",
    "payment",
    "account",
    "reminder",
    "statement",
    "subscription",
    "renewal",
    "address",
    "delivery",
    "tracking",
    "refund",
    "receipt",
    "update",
    "notification",
    "profile",
    "password",
    "session",
    "device",
    "invoice",
    "quarterly",
    "summary",
    "report",
    "attachment",
    "support",
    "ticket",
    "priority",
    "escalated",
    "resolved",
    "pending",
    "review",
    "approval",
]


def make_payload(i: int, size: int, *, sent_at: float | None = None) -> dict[str, Any]:
    """A JSON-serializable payload that is about ``size`` bytes once serialized."""
    rng = random.Random(i)
    doc: dict[str, Any] = {
        "n": i,
        "kind": "email.transactional",
        "recipient": f"user{i % 10_000}@example.com",
        "subject": f"Update {i} for your account",
        "body": "",
    }
    if sent_at is not None:
        doc["sent_at"] = sent_at
    overhead = len(json.dumps(doc))
    words: list[str] = []
    length = 0
    while length < size - overhead:
        word = rng.choice(_VOCAB)
        words.append(word)
        length += len(word) + 1
    doc["body"] = " ".join(words)[: max(0, size - overhead)]
    return doc
