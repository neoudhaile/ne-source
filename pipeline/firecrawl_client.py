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
LINK_DISCOVERY_KEYWORDS = (
    'about',
    'team',
    'leadership',
    'management',
    'executive',
    'staff',
    'directory',
    'contact',
)


def has_api_key() -> bool:
    return bool(os.getenv('ZYTE_API_KEY'))


def candidate_urls(website: str) -> list[str]:
    parsed = urlparse(website)
    base = f'{parsed.scheme}://{parsed.netloc}'
    paths = [
        '',
        '/',
        '/about',
        '/about-us',
        '/team',
        '/leadership',
        '/management',
        '/staff',
        '/directory',
        '/contact',
        '/contact-us',
    ]
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


def _extract_relevant_links(html: str, base_url: str) -> list[str]:
    links = []
    seen = set()
    for match in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html):
        href = unescape(match.group(1)).strip()
        if not href or href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
            continue
        text = _clean_html(match.group(2)).lower()
        absolute = urljoin(base_url, href)
        parsed_base = urlparse(base_url)
        parsed_link = urlparse(absolute)
        if parsed_link.netloc and parsed_link.netloc != parsed_base.netloc:
            continue
        haystack = f'{href} {text}'.lower()
        if not any(keyword in haystack for keyword in LINK_DISCOVERY_KEYWORDS):
            continue
        normalized = absolute.split('#', 1)[0]
        if normalized not in seen:
            seen.add(normalized)
            links.append(normalized)
    return links[:12]


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
            'links': _extract_relevant_links(html, response.url),
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
            'links': _extract_relevant_links(body, url),
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


def scrape_site_pages(website: str, max_pages: int = 8) -> list[dict]:
    pages = []
    queue = candidate_urls(website)
    seen = set()
    while queue and len(pages) < max_pages:
        url = queue.pop(0)
        normalized_url = url.rstrip('/') or url
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        try:
            data = scrape_url(url)
        except Exception:
            continue
        markdown = data.get('markdown') or data.get('content') or ''
        if not markdown:
            continue
        metadata = data.get('metadata') or {}
        pages.append({
            'url': data.get('url') or url,
            'markdown': markdown,
            'metadata': metadata,
            'provider_used': data.get('provider_used'),
            'fallback_reason': data.get('fallback_reason'),
        })
        discovered = []
        for link in metadata.get('links') or []:
            normalized_link = link.rstrip('/') or link
            if normalized_link not in seen and link not in queue:
                discovered.append(link)
        queue = discovered + queue
    return pages
