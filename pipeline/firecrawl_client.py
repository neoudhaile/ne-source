import base64
import os
import re
from html import unescape
from urllib.parse import urljoin, urlparse

import requests
from dotenv import load_dotenv

from pipeline.config import (
    SCRAPE_BLOCK_PATTERNS,
    SCRAPE_DIRECT_TIMEOUT,
    SCRAPE_ENABLE_ZYTE_FALLBACK,
    SCRAPE_MIN_TEXT_LENGTH,
    SCRAPE_ZYTE_TIMEOUT,
)

load_dotenv()

ZYTE_URL = 'https://api.zyte.com/v1/extract'
DEFAULT_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def has_api_key() -> bool:
    return bool(os.getenv('ZYTE_API_KEY'))


def candidate_urls(website: str) -> list[str]:
    parsed = urlparse(website)
    base = f'{parsed.scheme}://{parsed.netloc}'
    paths = ['', '/', '/contact', '/contact-us', '/about', '/about-us', '/team']
    urls = []
    seen = set()
    for path in paths:
        url = urljoin(base, path)
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def _clean_html(html: str) -> str:
    text = re.sub(r'(?is)<script[^>]*>.*?</script>', ' ', html)
    text = re.sub(r'(?is)<style[^>]*>.*?</style>', ' ', text)
    text = re.sub(r'(?is)<noscript[^>]*>.*?</noscript>', ' ', text)
    text = re.sub(r'(?i)<br\s*/?>', '\n', text)
    text = re.sub(r'(?i)</p\s*>', '\n', text)
    text = re.sub(r'(?i)</div\s*>', '\n', text)
    text = re.sub(r'(?is)<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t\r\f\v]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def _extract_title(html: str) -> str | None:
    match = re.search(r'(?is)<title>(.*?)</title>', html)
    if not match:
        return None
    return re.sub(r'\s+', ' ', unescape(match.group(1))).strip()


def _fallback_reason(status_code: int | None, title: str | None, text: str) -> str | None:
    lowered = text.lower()
    title_lower = (title or '').lower()
    if status_code in (401, 403):
        return f'HTTP {status_code}'
    for pattern in SCRAPE_BLOCK_PATTERNS:
        if pattern in lowered or pattern in title_lower:
            return f'blocked:{pattern}'
    if len(text) < SCRAPE_MIN_TEXT_LENGTH:
        return 'too_short'
    return None


def _direct_fetch(url: str) -> tuple[dict | None, str | None]:
    try:
        response = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=SCRAPE_DIRECT_TIMEOUT,
            allow_redirects=True,
        )
    except Exception as e:
        return None, f'direct_error:{e}'

    content_type = (response.headers.get('Content-Type') or '').lower()
    if 'html' not in content_type and 'text' not in content_type:
        return None, f'unsupported_content_type:{content_type or "unknown"}'

    html = response.text or ''
    title = _extract_title(html)
    cleaned = _clean_html(html)
    reason = _fallback_reason(response.status_code, title, cleaned)
    if reason:
        return None, reason

    return {
        'url': response.url,
        'markdown': cleaned,
        'metadata': {
            'status_code': response.status_code,
            'content_type': content_type,
            'title': title,
        },
        'provider_used': 'direct',
    }, None


def _zyte_auth_header() -> str:
    api_key = os.getenv('ZYTE_API_KEY')
    if not api_key:
        raise RuntimeError('ZYTE_API_KEY is not set')
    token = base64.b64encode(f'{api_key}:'.encode()).decode()
    return f'Basic {token}'


def _zyte_fetch(url: str, fallback_reason: str | None = None) -> dict:
    response = requests.post(
        ZYTE_URL,
        headers={
            'Authorization': _zyte_auth_header(),
            'Content-Type': 'application/json',
        },
        json={
            'url': url,
            'httpResponseBody': True,
            'httpResponseHeaders': True,
        },
        timeout=SCRAPE_ZYTE_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    body = base64.b64decode(data.get('httpResponseBody') or '').decode('utf-8', 'ignore')
    cleaned = _clean_html(body)
    title = _extract_title(body)
    return {
        'url': url,
        'markdown': cleaned,
        'metadata': {
            'title': title,
            'fallback_reason': fallback_reason,
        },
        'provider_used': 'direct_then_zyte' if fallback_reason else 'zyte',
        'fallback_reason': fallback_reason,
    }


def scrape_url(url: str) -> dict:
    page, reason = _direct_fetch(url)
    if page is not None:
        return page
    if not SCRAPE_ENABLE_ZYTE_FALLBACK or not has_api_key():
        raise RuntimeError(f'Direct scrape failed: {reason}')
    return _zyte_fetch(url, fallback_reason=reason)


def scrape_site_pages(website: str, max_pages: int = 4) -> list[dict]:
    pages = []
    for url in candidate_urls(website)[:max_pages]:
        try:
            data = scrape_url(url)
        except Exception:
            continue
        markdown = data.get('markdown') or data.get('content') or ''
        if not markdown:
            continue
        pages.append({
            'url': data.get('url') or url,
            'markdown': markdown,
            'metadata': data.get('metadata') or {},
            'provider_used': data.get('provider_used'),
            'fallback_reason': data.get('fallback_reason'),
        })
    return pages
