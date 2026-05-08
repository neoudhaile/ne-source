"""Domain recovery: Openmart first, optional Orth skill/Google CSE fallback.

Openmart can recover websites from company/location queries without needing a
separate ORTH_FIND_WEBSITE_PATH. A legacy Orth path and Google CSE remain as
fallbacks when configured.
"""

from __future__ import annotations

import itertools
import logging
import os
import re
from urllib.parse import unquote, urlparse

import requests
from dotenv import load_dotenv

from pipeline.config import (
    DOMAIN_RECOVERY_TIMEOUT,
    DOMAIN_VERIFICATION_MIN_TOKEN_OVERLAP,
    ENABLE_OPENMART_DOMAIN_RECOVERY,
)
from pipeline.firecrawl_client import _direct_fetch, _zyte_fetch, has_api_key

load_dotenv()

logger = logging.getLogger(__name__)

ORTH_BASE = 'https://x402.orth.sh'
GOOGLE_CSE_URL = 'https://www.googleapis.com/customsearch/v1'

REJECT_DOMAINS = {
    'facebook.com', 'x.com', 'twitter.com', 'linkedin.com', 'instagram.com',
    'youtube.com', 'tiktok.com', 'pinterest.com',
    'google.com', 'maps.google.com', 'mapquest.com', 'yelp.com',
    'yellowpages.com', 'manta.com', 'bbb.org', 'dnb.com',
    'glassdoor.com', 'indeed.com', 'angi.com', 'thumbtack.com',
    'amazon.com', 'ebay.com', 'etsy.com', 'shopify.com',
}

STREET_WORDS = {
    'street', 'st', 'avenue', 'ave', 'road', 'rd', 'boulevard', 'blvd',
    'drive', 'dr', 'lane', 'ln', 'way', 'place', 'pl', 'court', 'ct',
    'parkway', 'pkwy', 'circle', 'cir',
}

LINKEDIN_SLUG_STOPWORDS = {
    'inc', 'llc', 'co', 'company', 'corp', 'corporation', 'the', 'and',
    'of', 'ltd', 'limited',
}


def _root_domain(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain


def _is_rejected(url: str) -> bool:
    domain = _root_domain(url)
    if not domain:
        return True
    return domain in REJECT_DOMAINS or any(domain.endswith('.' + bad) for bad in REJECT_DOMAINS)


def _ensure_url(value: str) -> str:
    value = str(value or '').strip()
    if value and not value.lower().startswith(('http://', 'https://')):
        value = f'https://{value}'
    return value


def _filter_business_candidates(urls: list[str]) -> list[str]:
    filtered = []
    seen = set()
    for url in urls:
        url = _ensure_url(url)
        if not url or _is_rejected(url):
            continue
        canonical = _canonical(url)
        if canonical in seen:
            continue
        seen.add(canonical)
        filtered.append(url)
    return filtered


def _guess_domain_candidates(company: str | None, company_linkedin: str | None = None) -> list[str]:
    token_sets = []
    for tokens in (list(_company_tokens(company)), _linkedin_company_terms(company_linkedin)):
        useful = [token for token in tokens if len(token) > 2]
        if useful:
            token_sets.append(useful)

    guesses = []
    for tokens in token_sets:
        prefixes = [''.join(tokens), '-'.join(tokens)]
        if len(tokens) > 2:
            prefixes.extend([''.join(tokens[:2]), ''.join(tokens[:3])])
        for prefix, tld in itertools.product(prefixes, ('com', 'net', 'org')):
            if 4 <= len(prefix) <= 48:
                guesses.append(f'https://www.{prefix}.{tld}')
    return _filter_business_candidates(guesses)


def _canonical(url: str) -> str:
    url = _ensure_url(url)
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    return f'{parsed.scheme}://{parsed.netloc}'


def _linkedin_company_terms(company_linkedin: str | None) -> list[str]:
    parsed = urlparse(str(company_linkedin or '').strip())
    path = parsed.path if parsed.netloc else str(company_linkedin or '')
    marker = '/company/'
    if marker in path:
        slug = path.split(marker, 1)[1].split('/', 1)[0]
    else:
        slug = path.strip('/').split('/', 1)[0]
    slug = unquote(slug).replace('&', ' and ')
    slug = re.sub(r'[^a-zA-Z0-9]+', ' ', slug)
    terms = [
        term.lower()
        for term in slug.split()
        if len(term) >= 2 and not term.isdigit() and term.lower() not in LINKEDIN_SLUG_STOPWORDS
    ]
    return terms


def _location_context(address=None, city=None, state=None, zipcode=None) -> str:
    parts = []
    if address:
        parts.append(str(address))
    for part in (city, state, zipcode):
        if part:
            parts.append(str(part))
    return ', '.join(parts)


def _query(company: str, address=None, city=None, state=None, company_linkedin=None, zipcode=None) -> str:
    parts = [company]
    linkedin_terms = _linkedin_company_terms(company_linkedin)
    if linkedin_terms:
        parts.append(' '.join(linkedin_terms))
    location = _location_context(address=address, city=city, state=state, zipcode=zipcode)
    if location:
        parts.append(location)
    return ' '.join(part for part in parts if part)


def _query_variants(company: str, address=None, city=None, state=None, company_linkedin=None, zipcode=None) -> list[str]:
    variants = []
    location = _location_context(address=address, city=city, state=state, zipcode=zipcode)
    linkedin_terms = _linkedin_company_terms(company_linkedin)
    linkedin_text = ' '.join(linkedin_terms)
    for parts in (
        (company, linkedin_text, location),
        (company, location),
        (linkedin_text, location),
        (company, linkedin_text, city, state, zipcode),
    ):
        query = ' '.join(str(part) for part in parts if part)
        if query and query not in variants:
            variants.append(query)
    return variants


def _orth_search(query: str) -> list[str]:
    path = os.getenv('ORTH_FIND_WEBSITE_PATH', '').strip()
    if not path:
        return []

    try:
        api = os.getenv('ORTH_FIND_WEBSITE_API', '').strip()
        if api:
            from pipeline.orthogonal import run_api
            data = run_api(
                api,
                path,
                query={'q': query, 'limit': 5},
                timeout=DOMAIN_RECOVERY_TIMEOUT,
            ) or {}
        else:
            from pipeline.enrichment import _x402_session
            resp = _x402_session().get(
                f'{ORTH_BASE}{path}',
                params={'q': query, 'limit': 5},
                timeout=DOMAIN_RECOVERY_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json() or {}
    except Exception as exc:
        logger.warning('Orth website search failed: %s', exc)
        return []

    candidates = []
    for key in ('results', 'items', 'links', 'urls'):
        items = data.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, str):
                candidates.append(item)
            elif isinstance(item, dict):
                for url_key in ('url', 'link', 'href', 'website'):
                    if item.get(url_key):
                        candidates.append(str(item[url_key]))
                        break
    return candidates


def _record_urls(record: dict) -> list[str]:
    urls = []
    for key in (
        'website', 'website_url', 'company_website', 'url', 'domain',
        'homepage', 'site', 'raw_associated_website',
    ):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            urls.append(value.strip())
    for key in ('business', 'company', 'profile'):
        nested = record.get(key)
        if isinstance(nested, dict):
            urls.extend(_record_urls(nested))
    content = record.get('content')
    if isinstance(content, dict):
        urls.extend(_record_urls(content))
    for source in (record.get('from_sources') or {}).values() if isinstance(record.get('from_sources'), dict) else []:
        if isinstance(source, dict):
            urls.extend(_record_urls(source))
    return urls


def _openmart_search(query: str) -> list[str]:
    if not ENABLE_OPENMART_DOMAIN_RECOVERY:
        return []
    from pipeline.openmart import search_business_records

    candidates = []
    for record in search_business_records(query, page_size=5):
        candidates.extend(_record_urls(record))
    return candidates


def _cse_search(query: str) -> list[str]:
    api_key = os.getenv('GOOGLE_CSE_API_KEY', '').strip()
    cse_id = os.getenv('GOOGLE_CSE_ID', '').strip()
    if not api_key or not cse_id:
        return []
    try:
        resp = requests.get(
            GOOGLE_CSE_URL,
            params={'key': api_key, 'cx': cse_id, 'q': query, 'num': 5},
            timeout=DOMAIN_RECOVERY_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json() or {}
    except Exception as exc:
        logger.warning('Google CSE website search failed: %s', exc)
        return []
    return [str(item['link']) for item in data.get('items') or [] if item.get('link')]


def _fetch_page_text(url: str) -> str:
    page, reason = _direct_fetch(url)
    if page is None and has_api_key():
        try:
            page = _zyte_fetch(url, fallback_reason=reason)
        except Exception:
            page = None
    if not page:
        return ''
    return str(page.get('markdown') or page.get('content') or page.get('text') or '')


def _company_tokens(company: str | None) -> set[str]:
    text = re.sub(r'[^a-z0-9\s]', ' ', str(company or '').lower())
    stopwords = {'inc', 'llc', 'co', 'company', 'corp', 'corporation', 'the', 'and'}
    return {token for token in text.split() if len(token) > 2 and token not in stopwords}


def _address_parts(address: str | None) -> tuple[str | None, set[str]]:
    text = re.sub(r'[^a-z0-9\s]', ' ', str(address or '').lower())
    tokens = text.split()
    number = next((token for token in tokens if token.isdigit()), None)
    streets = {token for token in tokens if len(token) > 2 and token not in STREET_WORDS and not token.isdigit()}
    return number, streets


def _verify_candidate(
    url: str,
    company: str,
    address=None,
    company_linkedin=None,
    city=None,
    state=None,
    zipcode=None,
) -> bool:
    text = _fetch_page_text(url).lower()
    if not text:
        return False
    text_tokens = set(re.findall(r'[a-z0-9]+', text))

    number, streets = _address_parts(address)
    if number and number in text_tokens and any(street in text_tokens for street in streets):
        return True

    tokens = _company_tokens(company)
    tokens.update(_linkedin_company_terms(company_linkedin))
    overlap = sum(1 for token in tokens if token in text_tokens)
    location_hits = 0
    if city:
        city_tokens = [token for token in re.findall(r'[a-z0-9]+', str(city).lower()) if len(token) > 2]
        if city_tokens and all(token in text_tokens for token in city_tokens):
            location_hits += 1
    if state and str(state).lower() in text:
        location_hits += 1
    if zipcode and str(zipcode) in text_tokens:
        location_hits += 1
    if overlap >= 1 and location_hits:
        return True

    required_overlap = 2 if len(tokens) > 1 else DOMAIN_VERIFICATION_MIN_TOKEN_OVERLAP
    if overlap >= DOMAIN_VERIFICATION_MIN_TOKEN_OVERLAP:
        if overlap >= required_overlap:
            return True
        if all(len(token) > 3 for token in tokens):
            return True

    company_text = str(company or '').strip()
    if len(company_text) <= 3 and overlap < 2:
        return False

    if overlap >= DOMAIN_VERIFICATION_MIN_TOKEN_OVERLAP:
        return True

    linkedin = str(company_linkedin or '').strip().lower()
    return bool(linkedin and linkedin in text)


def find_company_website_with_provider(
    company: str,
    address: str | None = None,
    city: str | None = None,
    state: str | None = None,
    company_linkedin: str | None = None,
    zipcode: str | None = None,
) -> tuple[str | None, str | None, list[dict]]:
    """Return a verified canonical website URL, provider, and rejected candidates."""
    if not company:
        return None, None, []

    sources = [('openmart', _openmart_search)]
    if os.getenv('ORTH_FIND_WEBSITE_PATH', '').strip():
        sources.append(('orth_skill', _orth_search))
    sources.append(('google_cse', _cse_search))

    rejected = []
    seen = set()
    for provider, search_fn in sources:
        candidate_urls = []
        for query in _query_variants(
            company,
            address=address,
            city=city,
            state=state,
            company_linkedin=company_linkedin,
            zipcode=zipcode,
        ):
            candidate_urls.extend(search_fn(query))
        for candidate in _filter_business_candidates(candidate_urls):
            canonical = _canonical(candidate)
            if canonical in seen:
                continue
            seen.add(canonical)
            if _verify_candidate(
                candidate,
                company,
                address=address,
                company_linkedin=company_linkedin,
                city=city,
                state=state,
                zipcode=zipcode,
            ):
                return canonical, provider, rejected[:20]
            rejected.append({'provider': provider, 'url': canonical, 'reason': 'verification_failed'})

    for candidate in _guess_domain_candidates(company, company_linkedin=company_linkedin):
        canonical = _canonical(candidate)
        if canonical in seen:
            continue
        seen.add(canonical)
        if _verify_candidate(
            candidate,
            company,
            address=address,
            company_linkedin=company_linkedin,
            city=city,
            state=state,
            zipcode=zipcode,
        ):
            return canonical, 'domain_guess', rejected[:20]
        rejected.append({'provider': 'domain_guess', 'url': canonical, 'reason': 'verification_failed'})

    return None, None, rejected[:20]


def find_company_website(
    company: str,
    address: str | None = None,
    city: str | None = None,
    state: str | None = None,
    company_linkedin: str | None = None,
    zipcode: str | None = None,
) -> str | None:
    url, _provider, _rejected = find_company_website_with_provider(
        company,
        address=address,
        city=city,
        state=state,
        company_linkedin=company_linkedin,
        zipcode=zipcode,
    )
    return url
