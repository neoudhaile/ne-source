"""Openmart client helpers via Orthogonal.

Openmart is useful in two places in this pipeline:
- finding a likely business website from a company/location query
- enriching a known website/social profile for decision-maker contacts
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv

from pipeline.config import DOMAIN_RECOVERY_TIMEOUT
from pipeline.orthogonal import call_with_fallback

load_dotenv()

logger = logging.getLogger(__name__)

X402_OPENMART_BASE = 'https://x402.orth.sh/openmart'


def _unwrap_payload(payload: Any) -> Any:
    data = payload
    for key in ('data', 'result', 'results'):
        if isinstance(data, dict) and key in data:
            data = data[key]
    return data


def _call_openmart_x402(path: str, *, body: dict, query: dict) -> Any:
    from pipeline.enrichment import _x402_session

    resp = _x402_session().post(
        f'{X402_OPENMART_BASE}{path}',
        params=query,
        json=body,
        timeout=DOMAIN_RECOVERY_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _call_openmart(path: str, *, body: dict | None = None, query: dict | None = None) -> Any:
    """Call Openmart through Orthogonal first, with x402 fallback."""
    body = body or {}
    query = query or {}
    return call_with_fallback(
        'openmart',
        path,
        body=body,
        query=query,
        timeout=DOMAIN_RECOVERY_TIMEOUT,
        fallback=lambda: _call_openmart_x402(path, body=body, query=query),
    )


def search_business_records(
    query: str,
    *,
    page_size: int = 5,
    min_rating: float | None = None,
    min_reviews: int | None = None,
    cursor=None,
) -> list[dict]:
    if not query:
        return []
    body: dict[str, Any] = {'query': query, 'page_size': page_size}
    if min_rating is not None:
        body['min_rating'] = min_rating
    if min_reviews is not None:
        body['min_reviews'] = min_reviews
    if cursor is not None:
        body['cursor'] = cursor
    try:
        payload = _call_openmart('/api/v1/search', body=body)
    except Exception as exc:
        logger.warning('Openmart search failed: %s', exc)
        return []

    data = _unwrap_payload(payload)
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ('items', 'businesses', 'records'):
            items = data.get(key)
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
    return []


def enrich_company(*, company_website: str | None = None, social_media_link: str | None = None) -> dict:
    body = {}
    if company_website:
        body['company_website'] = company_website
    if social_media_link:
        body['social_media_link'] = social_media_link
    if not body:
        return {}
    try:
        payload = _call_openmart('/api/v1/enrich_company', body=body)
    except Exception as exc:
        logger.warning('Openmart enrich_company failed: %s', exc)
        return {}
    data = _unwrap_payload(payload)
    return data if isinstance(data, dict) else {}
