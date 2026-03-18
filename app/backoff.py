"""Shared retry/backoff helpers for transient network failures."""

from __future__ import annotations

import random


def parse_retry_after_or_default(
    retry_after: str | None,
    attempt: int,
    base_seconds: float = 3.0,
    jitter_max_seconds: float = 2.0,
) -> float:
    """Return `Retry-After` seconds when valid, otherwise a jittered default."""
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return base_seconds * (attempt + 1) + random.uniform(0, jitter_max_seconds)


def linear_backoff(attempt: int, step_seconds: float = 2.0) -> float:
    """Return linear backoff delay for zero-based retry attempts."""
    return step_seconds * (attempt + 1)
