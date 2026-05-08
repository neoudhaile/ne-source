"""Orthogonal Run API client with optional x402 fallback.

The enrichment pipeline should prefer Orthogonal prepaid credits when
ORTHOGONAL_API_KEY is available, while retaining x402 as a fallback during the
migration.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ORTHOGONAL_RUN_URL = os.getenv('ORTHOGONAL_RUN_URL', 'https://api.orthogonal.com/v1/run')


def has_api_key() -> bool:
    return bool(os.getenv('ORTHOGONAL_API_KEY', '').strip())


def _headers() -> dict[str, str]:
    return {
        'Authorization': f"Bearer {os.getenv('ORTHOGONAL_API_KEY', '').strip()}",
        'Content-Type': 'application/json',
    }


def _unwrap_run_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and payload.get('success') is True and 'data' in payload:
        return payload['data']
    return payload


def _stringify_query(query: dict | None) -> dict:
    if not query:
        return {}
    return {key: str(value) for key, value in query.items() if value is not None}


def run_api(
    api: str,
    path: str,
    *,
    query: dict | None = None,
    body: dict | None = None,
    timeout: int | float = 30,
) -> Any:
    if not has_api_key():
        raise RuntimeError('ORTHOGONAL_API_KEY is not configured')
    resp = requests.post(
        ORTHOGONAL_RUN_URL,
        json={
            'api': api,
            'path': path,
            'query': _stringify_query(query),
            'body': body or {},
        },
        headers=_headers(),
        timeout=timeout,
    )
    resp.raise_for_status()
    return _unwrap_run_payload(resp.json())


def call_with_fallback(
    api: str,
    path: str,
    *,
    query: dict | None = None,
    body: dict | None = None,
    timeout: int | float = 30,
    fallback: Callable[[], Any] | None = None,
) -> Any:
    """Prefer Orthogonal Run API, falling back to x402 if needed."""
    if has_api_key():
        try:
            return run_api(api, path, query=query, body=body, timeout=timeout)
        except requests.exceptions.Timeout:
            raise
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and 400 <= status < 500:
                raise
            logger.warning('Orthogonal run failed for %s %s: %s', api, path, exc)
            if fallback is None:
                raise
        except Exception as exc:
            logger.warning('Orthogonal run failed for %s %s: %s', api, path, exc)
            if fallback is None:
                raise
    if fallback is None:
        return run_api(api, path, query=query, body=body, timeout=timeout)
    return fallback()
