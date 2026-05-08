"""
Waterfall enrichment — 7 steps per lead.

Uses Orthogonal APIs for paid steps with x402 fallback, Claude API as failsafe.
Each step only populates fields that are still empty after previous steps.
"""

import logging
import os
import json
import re
import threading
import time
import traceback
import requests
import anthropic
from concurrent.futures import ThreadPoolExecutor, as_completed
from eth_account import Account
from x402 import x402ClientSync
from x402.mechanisms.evm.exact.v1.client import ExactEvmSchemeV1
from x402.http.clients.requests import x402_http_adapter
from dotenv import load_dotenv

from pipeline.config import ENABLE_REVIEW_SCRAPE, FAST_TIMEOUT, SLOW_TIMEOUT
from pipeline.db import get_lead, get_lead_by_google_place_id, update_lead
from pipeline.firecrawl_client import (
    has_api_key as has_firecrawl_api_key,
    scrape_site_pages,
    scrape_url,
)
from pipeline.google_places import find_place, get_place_details

load_dotenv()

# ── Error logger — writes to logs/enrichment_errors.log ──────────────────────
_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(_LOG_DIR, exist_ok=True)

_error_logger = logging.getLogger('enrichment_errors')
_error_logger.setLevel(logging.ERROR)
_error_handler = logging.FileHandler(os.path.join(_LOG_DIR, 'enrichment_errors.log'))
_error_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
_error_logger.addHandler(_error_handler)
_error_logger.propagate = False

# ── Claude client (for failsafe) ───────────────────────────────────────────
claude = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

ENRICHABLE_FIELDS = [
    'owner_name', 'owner_email', 'owner_phone', 'owner_linkedin',
    'employee_count', 'key_staff', 'year_established', 'services_offered',
    'company_description', 'revenue_estimate', 'certifications',
    'review_summary', 'facebook_url', 'yelp_url', 'google_maps_url',
]

ORTH_BASE = 'https://x402.orth.sh'

STEP_NAMES = {
    'google_places': 'Google Places',
    'google_maps': 'Google Maps URL',
    'domain_recovery': 'Domain recovery',
    'openmart_company': 'Openmart company enrich',
    'hunter': 'Hunter.io',
    'apollo': 'Apollo',
    'owner_email_followup': 'Owner email follow-up',
    'fullenrich': 'FullEnrich',
    'sixtyfour': 'Sixtyfour',
    'scrape_website': 'Website scrape',
    'scrape_reviews': 'Review scrape',
    'company_fallback': 'Company fallback',
    'claude_failsafe': 'Claude failsafe',
}

STEP_SKIP_REASONS = {
    'google_places': 'No company/address data available for place matching, or no place match found.',
    'google_maps': 'No real Google place ID available.',
    'domain_recovery': 'Lead already has a website, or no recovery providers configured.',
    'openmart_company': 'No website or social profile available for Openmart company enrich.',
    'hunter': 'No website/domain available for owner-contact lookup.',
    'apollo': 'No usable owner/company context remained for people matching.',
    'owner_email_followup': 'No grounded owner name and verified domain available for owner-email lookup.',
    'fullenrich': 'No FULLENRICH_API_KEY set, or all owner fields already filled.',
    'sixtyfour': 'Sixtyfour skipped — owner_phone already filled, or owner_name / verified domain missing.',
    'scrape_website': 'No website available to scrape, or no scraper fallback is configured.',
    'scrape_reviews': 'Review scrape disabled, or no review URL available to scrape.',
    'company_fallback': 'No company email or phone available to reuse.',
    'claude_failsafe': 'No missing non-contact fields remained to infer.',
}

LOW_VALUE_CLAUDE_FIELDS = {
    'review_summary',
    'facebook_url',
    'yelp_url',
}
HUNTER_RETRIES = 2
SIXTYFOUR_TIMEOUT = int(os.getenv('SIXTYFOUR_TIMEOUT', '90'))

OWNER_FIELD_SOURCE_PRIORITY = {
    'owner_name': {
        'hunter': 100,
        'scrape': 95,
        'apollo': 90,
        'apollo_search': 85,
        'openmart': 80,
        'fullenrich': 70,
        'sixtyfour': 65,
        'company_fallback': 10,
        'claude_inferred': 0,
    },
    'owner_email': {
        'hunter': 100,
        'apollo': 90,
        'openmart': 80,
        'fullenrich': 70,
        'sixtyfour': 60,
        'scrape': 50,
        'company_fallback': 10,
        'claude_inferred': 0,
    },
    'owner_phone': {
        'apollo': 100,
        'hunter': 90,
        'openmart': 80,
        'sixtyfour': 70,
        'fullenrich': 60,
        'scrape': 50,
        'claude_inferred': 0,
    },
    'owner_linkedin': {
        'hunter': 100,
        'apollo': 90,
        'apollo_search': 85,
        'openmart': 80,
        'fullenrich': 70,
        'sixtyfour': 60,
        'scrape': 50,
        'claude_inferred': 0,
    },
    'key_staff': {
        'apollo': 100,
        'hunter': 60,
        'openmart': 50,
        'fullenrich': 40,
        'sixtyfour': 35,
        'apollo_search': 30,
        'claude_inferred': 0,
    },
}

GENERIC_EMAIL_PREFIXES = {
    'info', 'sales', 'support', 'contact', 'hello', 'team', 'office', 'admin',
    'billing', 'careers', 'jobs', 'service', 'customerservice', 'customersupport',
}

OWNER_TITLE_KEYWORDS = (
    'owner', 'founder', 'co-founder', 'president', 'ceo', 'principal',
    'managing partner', 'partner', 'director', 'manager',
)

_x402_insufficient = False
_x402_consecutive_402s = 0
_X402_CONSECUTIVE_THRESHOLD = 3  # Flag as insufficient after 3 consecutive 402s


def reset_x402_flag():
    """Reset the insufficient funds flag. Used between runs and in tests."""
    global _x402_insufficient, _x402_consecutive_402s
    _x402_insufficient = False
    _x402_consecutive_402s = 0


def _claude_call_with_retry(fn, max_retries=3):
    """Call fn(), retrying on Claude 429 rate-limit errors.
    Respects retry-after header when available, otherwise uses exponential backoff."""
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except anthropic.RateLimitError as e:
            if attempt >= max_retries:
                raise
            # Try to read retry-after from the response headers
            retry_after = None
            resp = getattr(e, 'response', None)
            if resp is not None:
                retry_after = resp.headers.get('retry-after')
            if retry_after:
                try:
                    wait = float(retry_after)
                except (ValueError, TypeError):
                    wait = 5 * (2 ** attempt)
            else:
                wait = 5 * (2 ** attempt)  # 5s, 10s, 20s
            _error_logger.error('Claude 429 (attempt %d/%d), waiting %.1fs: %s', attempt + 1, max_retries, wait, e)
            time.sleep(wait)
        except Exception:
            raise


# ── Helpers ─────────────────────────────────────────────────────────────────

_thread_local = threading.local()

def _x402_session() -> requests.Session:
    """Return a thread-local x402-backed session (created once per thread)."""
    session = getattr(_thread_local, 'x402_session', None)
    if session is not None:
        return session
    account = Account.from_key(os.getenv('PRIVATE_KEY'))
    client = x402ClientSync()
    client.register_v1('base', ExactEvmSchemeV1(signer=account))
    session = requests.Session()
    session.mount('https://', x402_http_adapter(client))
    _thread_local.x402_session = session
    return session


# USDC contract on Base
_USDC_BASE_CONTRACT = '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
_BASE_RPC = 'https://mainnet.base.org'


def check_x402_balance() -> float:
    """Return USDC balance in dollars for the x402 wallet on Base.
    Returns 0.0 on any error."""
    try:
        account = Account.from_key(os.getenv('PRIVATE_KEY'))
        address = account.address
        # balanceOf(address) selector = 0x70a08231, left-padded to 32 bytes
        padded = address[2:].lower().zfill(64)
        data = '0x70a08231' + padded
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'eth_call',
            'params': [
                {'to': _USDC_BASE_CONTRACT, 'data': data},
                'latest',
            ],
        }
        # Plain requests.post — Base RPC is free, no x402 payment needed
        resp = requests.post(_BASE_RPC, json=payload, timeout=10)
        hex_balance = resp.json().get('result', '0x0')
        raw = int(hex_balance, 16)
        return raw / 1_000_000  # USDC has 6 decimals
    except Exception:
        return 0.0


def _has_orthogonal_api_key() -> bool:
    from pipeline.orthogonal import has_api_key
    return has_api_key()


def _paid_provider_available() -> bool:
    return not _x402_insufficient or _has_orthogonal_api_key()


def _record_x402_status(status_code: int):
    global _x402_insufficient, _x402_consecutive_402s
    if status_code == 402:
        _x402_consecutive_402s += 1
        if _x402_consecutive_402s >= _X402_CONSECUTIVE_THRESHOLD:
            _x402_insufficient = True
    else:
        _x402_consecutive_402s = 0


def _x402_get_json(api: str, path: str, *, query: dict | None = None, timeout: int | float = SLOW_TIMEOUT):
    session = _x402_session()
    resp = session.get(f'{ORTH_BASE}/{api}{path}', params=query or {}, timeout=timeout)
    _record_x402_status(resp.status_code)
    resp.raise_for_status()
    return resp.json()


def _x402_post_json(api: str, path: str, *, body: dict | None = None, timeout: int | float = SLOW_TIMEOUT):
    session = _x402_session()
    resp = session.post(f'{ORTH_BASE}/{api}{path}', json=body or {}, timeout=timeout)
    _record_x402_status(resp.status_code)
    resp.raise_for_status()
    return resp.json()


def _provider_get_json(
    api: str,
    path: str,
    *,
    query: dict | None = None,
    timeout: int | float = SLOW_TIMEOUT,
):
    from pipeline.orthogonal import call_with_fallback
    return call_with_fallback(
        api,
        path,
        query=query or {},
        timeout=timeout,
        fallback=lambda: _x402_get_json(api, path, query=query, timeout=timeout),
    )


def _provider_post_json(
    api: str,
    path: str,
    *,
    body: dict | None = None,
    timeout: int | float = SLOW_TIMEOUT,
):
    from pipeline.orthogonal import call_with_fallback
    return call_with_fallback(
        api,
        path,
        body=body or {},
        timeout=timeout,
        fallback=lambda: _x402_post_json(api, path, body=body, timeout=timeout),
    )


def _get_missing(lead: dict, enriched: dict) -> list[str]:
    """Return field names still empty in both lead and enriched."""
    missing = []
    for f in ENRICHABLE_FIELDS:
        val = enriched.get(f) or lead.get(f)
        if val is None or val == '' or val == []:
            missing.append(f)
    return missing


def _domain_from_website(website: str | None) -> str | None:
    if not website:
        return None
    domain = website.replace('https://', '').replace('http://', '').split('/')[0]
    return domain.lower().strip()


def _value(lead: dict, enriched: dict, key: str):
    return enriched.get(key) or lead.get(key)


def _is_empty(value) -> bool:
    return value is None or value == '' or value == []


def _field_source(meta: dict, field: str) -> str | None:
    info = meta.get(field) or {}
    return info.get('source') or info.get('provider')


def _source_priority(field: str, source: str | None) -> int:
    if not source:
        return 0
    return OWNER_FIELD_SOURCE_PRIORITY.get(field, {}).get(source, 0)


def _is_real_google_place_id(value: str | None) -> bool:
    return bool(value) and not str(value).startswith(('CSV_', 'BENCH_'))


def _looks_like_person_linkedin(value: str | None) -> bool:
    text = str(value or '').strip().lower()
    return text.startswith('http') and 'linkedin.com/in/' in text


def _looks_like_company_linkedin(value: str | None) -> bool:
    text = str(value or '').strip().lower()
    return text.startswith('http') and 'linkedin.com/company/' in text


def _grounded_field_present(lead: dict, enriched: dict, meta: dict, field: str) -> bool:
    value = _value(lead, enriched, field)
    if _is_empty(value):
        return False
    source = _field_source(meta, field)
    if source in {'claude_inferred', 'csv_import', None}:
        return field in {'website', 'company_phone', 'company_email'} and bool(lead.get(field))
    return True


def _has_grounded_evidence(lead: dict, enriched: dict, meta: dict) -> bool:
    if _is_real_google_place_id(_value(lead, enriched, 'google_place_id')):
        return True
    grounded_fields = (
        'website',
        'company_phone',
        'company_email',
        'owner_name',
        'owner_email',
        'services_offered',
        'company_description',
    )
    return any(_grounded_field_present(lead, enriched, meta, field) for field in grounded_fields)


def _generic_email(email: str | None) -> bool:
    if not email or '@' not in email:
        return True
    local = email.split('@', 1)[0].lower()
    return local in GENERIC_EMAIL_PREFIXES


def _truthful_owner_email_present(lead: dict, enriched: dict, meta: dict) -> bool:
    email = _value(lead, enriched, 'owner_email')
    if not email:
        return False
    source = _field_source(meta, 'owner_email')
    return source not in {None, 'csv_import', 'claude_inferred', 'company_fallback'}


def _email_matches_domain(email: str | None, domain: str | None) -> bool:
    if not email or '@' not in str(email) or not domain:
        return False
    email_domain = str(email).split('@', 1)[1].lower().strip()
    domain = str(domain).lower().strip()
    return email_domain == domain or email_domain.endswith('.' + domain)


def _name_quality(name: str | None) -> int:
    if not name:
        return 0
    tokens = [token for token in str(name).strip().split() if token]
    score = len(tokens) * 10
    if len(tokens) >= 2:
        score += 20
    if all(any(ch.isalpha() for ch in token) for token in tokens):
        score += 5
    return score


def _same_person_name(left: str | None, right: str | None) -> bool:
    left_tokens = [token.lower() for token in re.findall(r'[a-z]+', str(left or ''))]
    right_tokens = [token.lower() for token in re.findall(r'[a-z]+', str(right or ''))]
    if len(left_tokens) < 2 or len(right_tokens) < 2:
        return False
    return left_tokens[0] == right_tokens[0] and left_tokens[-1] == right_tokens[-1]


def _email_quality(email: str | None) -> int:
    if not email:
        return 0
    score = 20
    if '@' in email:
        score += 20
    if not _generic_email(email):
        score += 40
    return score


def _should_replace(field: str, current_value, current_meta: dict, new_value, new_source: str | None) -> bool:
    if _is_empty(new_value):
        return False
    if _is_empty(current_value):
        return True
    if field not in OWNER_FIELD_SOURCE_PRIORITY:
        return False

    current_source = _field_source(current_meta, field)
    current_priority = _source_priority(field, current_source)
    new_priority = _source_priority(field, new_source)
    if new_priority > current_priority:
        return True
    if new_priority < current_priority:
        return False

    if field == 'owner_email':
        return _email_quality(str(new_value)) > _email_quality(str(current_value))
    if field == 'owner_name':
        return _name_quality(str(new_value)) > _name_quality(str(current_value))
    if field == 'key_staff':
        current_len = len(current_value) if isinstance(current_value, list) else 0
        new_len = len(new_value) if isinstance(new_value, list) else 0
        return new_len > current_len
    return False


def _company_tokens(company: str | None) -> set[str]:
    text = str(company or '').lower()
    text = ''.join(ch if ch.isalnum() else ' ' for ch in text)
    stopwords = {'inc', 'llc', 'co', 'company', 'corporation', 'corp', 'the'}
    return {token for token in text.split() if len(token) > 2 and token not in stopwords}


def _relevant_pages_for_company(company: str | None, pages: list[dict]) -> list[dict]:
    tokens = _company_tokens(company)
    if not tokens:
        return pages
    relevant = []
    for page in pages:
        haystack = ' '.join([
            str(page.get('url') or ''),
            str(page.get('markdown') or '')[:4000],
        ]).lower()
        matches = sum(1 for token in tokens if token in haystack)
        if matches >= min(2, len(tokens)):
            relevant.append(page)
    return relevant or pages


def _merge(enriched: dict, meta: dict, result: dict, source: str):
    """Merge non-empty values from result into enriched, tag in meta."""
    for k, v in result.items():
        if _is_empty(v):
            continue
        current_value = enriched.get(k)
        if _should_replace(k, current_value, meta, v, source):
            enriched[k] = v
            meta[k] = {'source': source}
            continue
        if k not in enriched or _is_empty(current_value):
            enriched[k] = v
            meta[k] = {'source': source}


def _tag_fields(meta: dict, fields: list[str], provider_used: str | None):
    if not provider_used:
        return
    for field in fields:
        if field in meta:
            meta[field]['provider'] = provider_used


def _step_company_contact_fallback(lead: dict, enriched: dict, meta: dict) -> float:
    """
    Prefer owner-specific contact data from the paid steps, but if none was found,
    use company-level email so the table does not stay blank.

    Company phones are intentionally not promoted to owner_phone; that would
    inflate owner-phone coverage with business-line data.
    """
    fallback = {}
    company_email = _value(lead, enriched, 'company_email')
    if not _value(lead, enriched, 'owner_email') and company_email:
        fallback['owner_email'] = company_email
    assert 'owner_phone' not in fallback
    if fallback:
        for field, value in fallback.items():
            enriched[field] = value
            meta[field] = {'source': 'company_fallback', 'fallback': True}
    return 0.0


# ── Step 2: Google Places details/match ─────────────────────────────────────

def _step_google_places(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['website', 'company_phone', 'google_maps_url', 'rating', 'review_count']
    missing = [f for f in target_fields if not _value(lead, enriched, f)]
    if not missing and _value(lead, enriched, 'google_place_id'):
        return 0.0

    details = None
    place_id = _value(lead, enriched, 'google_place_id')
    if place_id and not str(place_id).startswith('CSV_'):
        try:
            details = get_place_details(str(place_id))
        except Exception:
            details = None

    if details is None:
        company = _value(lead, enriched, 'company')
        address = _value(lead, enriched, 'address')
        city = _value(lead, enriched, 'city')
        state = _value(lead, enriched, 'state')
        if not company:
            return 0.0
        details = find_place(str(company), address=address, city=city, state=state)
        if not details:
            return 0.0

    display_name = details.get('displayName') or {}
    matched_place_id = details.get('id') or place_id
    existing = None
    if matched_place_id and not str(matched_place_id).startswith('CSV_'):
        existing = get_lead_by_google_place_id(str(matched_place_id))
        if existing and existing.get('id') == lead.get('id'):
            existing = None
    result = {
        'google_place_id': matched_place_id,
        'google_maps_url': details.get('googleMapsUri'),
        'website': details.get('websiteUri'),
        'company_phone': details.get('nationalPhoneNumber'),
        'rating': details.get('rating'),
        'review_count': details.get('userRatingCount'),
        'company': _value(lead, enriched, 'company') or display_name.get('text'),
        'address': _value(lead, enriched, 'address') or details.get('formattedAddress'),
    }
    if existing:
        result.pop('google_place_id', None)
    _merge(enriched, meta, result, 'google_places')
    return 0.0


# ── Step 3: Construct google_maps_url (free) ───────────────────────────────

def _step_google_maps(lead: dict, enriched: dict, meta: dict) -> float:
    place_id = _value(lead, enriched, 'google_place_id')
    if (
        place_id and
        _is_real_google_place_id(str(place_id)) and
        not _value(lead, enriched, 'google_maps_url')
    ):
        enriched['google_maps_url'] = f'https://www.google.com/maps/place/?q=place_id:{place_id}'
        meta['google_maps_url'] = {'source': 'constructed'}
    return 0.0


# ── Step: Domain recovery — verified website lookup ────────────────────────

def _step_domain_recovery(lead: dict, enriched: dict, meta: dict) -> float:
    if _value(lead, enriched, 'website'):
        return 0.0

    from pipeline.config import ENABLE_DOMAIN_RECOVERY
    if not ENABLE_DOMAIN_RECOVERY:
        meta['__skip_reason'] = 'Domain recovery disabled.'
        return 0.0

    from pipeline.config import ENABLE_OPENMART_DOMAIN_RECOVERY
    if not (
        ENABLE_OPENMART_DOMAIN_RECOVERY
        or os.getenv('ORTH_FIND_WEBSITE_PATH', '').strip()
        or (os.getenv('GOOGLE_CSE_API_KEY', '').strip() and os.getenv('GOOGLE_CSE_ID', '').strip())
    ):
        meta['__skip_reason'] = (
            'Domain recovery skipped — Openmart disabled and neither '
            'ORTH_FIND_WEBSITE_PATH nor GOOGLE_CSE_API_KEY+GOOGLE_CSE_ID is configured.'
        )
        return 0.0

    from pipeline.google_search import find_company_website_with_provider
    url, provider, rejected = find_company_website_with_provider(
        company=_value(lead, enriched, 'company'),
        address=_value(lead, enriched, 'address'),
        city=_value(lead, enriched, 'city'),
        state=_value(lead, enriched, 'state'),
        company_linkedin=_value(lead, enriched, 'company_linkedin'),
        zipcode=_value(lead, enriched, 'zipcode'),
    )
    if not url:
        if rejected:
            summary = ', '.join(
                f"{item.get('provider')}:{item.get('url')}"
                for item in rejected[:5]
            )
            meta['domain_recovery'] = {
                'source': 'domain_recovery',
                'rejected_candidates': rejected[:10],
            }
            meta['__skip_reason'] = f'No verified website/domain found. Rejected candidates: {summary}'
        else:
            meta['__skip_reason'] = 'No verified website/domain found; no business-domain candidates returned.'
        return 0.0

    enriched['website'] = url
    meta['website'] = {'source': 'domain_recovery', 'provider': provider or 'unknown'}
    return 0.0


# ── Step: Openmart company enrichment ───────────────────────────────────────

OPENMART_COST_PER_CALL = 0.01
OPENMART_OWNER_ROLE_KEYWORDS = (
    'owner', 'founder', 'co-founder', 'president', 'ceo', 'chief executive',
    'principal', 'managing partner', 'partner', 'general manager',
    'executive director', 'district manager',
)


def _walk_dicts(value):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_dicts(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_dicts(item)


def _first_present(record: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _name_from_record(record: dict) -> str | None:
    full = _first_present(record, ('owner_name', 'full_name', 'name', 'person_name', 'contact_name'))
    if full:
        return full
    first = _first_present(record, ('first_name', 'firstname', 'given_name'))
    last = _first_present(record, ('last_name', 'lastname', 'family_name'))
    return ' '.join(part for part in (first, last) if part) or None


def _openmart_role_score(role: str | None) -> int:
    text = str(role or '').lower()
    if not text:
        return 0
    for keyword in OPENMART_OWNER_ROLE_KEYWORDS:
        if keyword in text:
            return 100
    return 20


def _extract_openmart_owner_contact(data: dict) -> dict:
    """Extract explicit person/decision-maker fields without guessing."""
    direct = {
        'owner_name': _first_present(data, ('owner_name',)),
        'owner_email': _first_present(data, ('owner_email',)),
        'owner_phone': _first_present(data, ('owner_phone',)),
        'owner_linkedin': _first_present(data, ('owner_linkedin', 'owner_linkedin_url')),
    }
    direct = {k: v for k, v in direct.items() if v}
    if direct:
        return direct

    candidates = []
    for record in _walk_dicts(data):
        name = _name_from_record(record)
        email = _first_present(record, ('email', 'work_email', 'verified_email', 'business_email'))
        phone = _first_present(record, ('phone', 'phone_number', 'mobile_phone', 'direct_phone', 'work_phone'))
        linkedin = _first_present(record, ('linkedin', 'linkedin_url', 'person_linkedin_url'))
        role = _first_present(record, ('title', 'position', 'job_title', 'role'))
        if not any((name, email, phone, linkedin)):
            continue
        role_score = _openmart_role_score(role)
        if not role_score and not (name and (email or linkedin)):
            continue
        score = role_score
        score += 30 if name else 0
        score += 30 if email and not _generic_email(email) else 0
        score += 20 if phone else 0
        score += 10 if linkedin else 0
        candidates.append((score, {
            'owner_name': name,
            'owner_email': email,
            'owner_phone': phone,
            'owner_linkedin': linkedin,
        }))

    if not candidates:
        return {}
    candidates.sort(key=lambda item: item[0], reverse=True)
    return {k: v for k, v in candidates[0][1].items() if v}


APOLLO_DECISION_MAKER_TITLES = [
    'Owner',
    'Founder',
    'Co-Founder',
    'President',
    'CEO',
    'Chief Executive Officer',
    'Principal',
    'Managing Partner',
    'Partner',
    'General Manager',
    'Executive Director',
    'District Manager',
    'Operations Director',
]


def _person_company_name(record: dict) -> str | None:
    org = record.get('organization') or record.get('account') or {}
    if isinstance(org, dict):
        return _first_present(org, ('name', 'organization_name', 'company_name'))
    return _first_present(record, ('organization_name', 'company_name', 'company'))


def _person_company_domain(record: dict) -> str | None:
    org = record.get('organization') or record.get('account') or {}
    if isinstance(org, dict):
        return _first_present(org, ('primary_domain', 'domain', 'website_url'))
    return _first_present(record, ('organization_domain', 'domain', 'website_url'))


def _company_relevance_score(company: str | None, domain: str | None, record: dict) -> int:
    score = 0
    person_domain = _person_company_domain(record)
    if domain and person_domain and domain.lower() in person_domain.lower():
        score += 60
    target_tokens = _company_tokens(company)
    org_name = _person_company_name(record)
    if target_tokens and org_name:
        org_tokens = _company_tokens(org_name)
        overlap = len(target_tokens & org_tokens)
        if overlap:
            score += min(40, overlap * 20)
    return score


def _apollo_decision_role_score(title: str | None) -> int:
    text = str(title or '').lower()
    if not text:
        return 0
    ranked = (
        ('owner', 100),
        ('founder', 100),
        ('co-founder', 100),
        ('president', 85),
        ('chief executive', 80),
        ('ceo', 80),
        ('principal', 75),
        ('managing partner', 75),
        ('partner', 70),
        ('general manager', 70),
        ('executive director', 65),
        ('district manager', 60),
        ('director', 45),
        ('manager', 35),
    )
    return max((score for keyword, score in ranked if keyword in text), default=0)


def _extract_apollo_people(data: dict) -> list[dict]:
    people = []
    for key in ('people', 'contacts', 'persons'):
        value = data.get(key)
        if isinstance(value, list):
            people.extend(item for item in value if isinstance(item, dict))
    person = data.get('person')
    if isinstance(person, dict):
        people.append(person)
    return people


def _apollo_person_contact(record: dict) -> dict:
    result = {}
    name = _first_present(record, ('name', 'full_name')) or _name_from_record(record)
    if name and _name_quality(name) < 40:
        name = None
    title = _first_present(record, ('title', 'headline', 'position'))
    linkedin = _first_present(record, ('linkedin_url', 'linkedin'))
    email = _first_present(record, ('email', 'work_email'))
    if name:
        result['owner_name'] = name
    if email and not _generic_email(email):
        result['owner_email'] = email
    if linkedin and _looks_like_person_linkedin(linkedin):
        result['owner_linkedin'] = linkedin
    phones = record.get('phone_numbers') or []
    if isinstance(phones, list) and phones:
        phone = phones[0].get('sanitized_number') or phones[0].get('raw_number')
        if phone:
            result['owner_phone'] = str(phone)
    if title and name:
        result['key_staff'] = [f'{name} — {title}']
    elif name:
        result['key_staff'] = [name]
    return result


def _search_apollo_decision_maker(lead: dict, enriched: dict) -> dict:
    company = _value(lead, enriched, 'company')
    domain = _domain_from_website(_value(lead, enriched, 'website'))
    if not company and not domain:
        return {}

    locations = [
        location for location in (
            ', '.join(part for part in (_value(lead, enriched, 'city'), _value(lead, enriched, 'state')) if part),
            _value(lead, enriched, 'state'),
        )
        if location
    ]
    payload = {
        'person_titles': APOLLO_DECISION_MAKER_TITLES,
        'person_seniorities': ['owner', 'founder', 'c_suite', 'partner', 'vp', 'head', 'director', 'manager'],
        'q_keywords': company or domain,
        'page': 1,
        'per_page': 5,
    }
    if locations:
        payload['organization_locations'] = locations
        payload['person_locations'] = locations

    data = _provider_post_json(
        'apollo',
        '/api/v1/mixed_people/api_search',
        body=payload,
        timeout=SLOW_TIMEOUT,
    )
    candidates = []
    for person in _extract_apollo_people(data or {}):
        title = _first_present(person, ('title', 'headline', 'position'))
        role_score = _apollo_decision_role_score(title)
        relevance = _company_relevance_score(company, domain, person)
        contact = _apollo_person_contact(person)
        if not contact.get('owner_name') or (role_score < 35 and relevance < 40):
            continue
        score = role_score + relevance
        score += 15 if contact.get('owner_linkedin') else 0
        score += 20 if contact.get('owner_email') else 0
        candidates.append((score, contact))
    if not candidates:
        return {}
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _step_openmart_company(lead: dict, enriched: dict, meta: dict) -> float:
    from pipeline.config import ENABLE_OPENMART_COMPANY_ENRICH

    if not ENABLE_OPENMART_COMPANY_ENRICH:
        meta['__skip_reason'] = 'Openmart company enrich disabled.'
        return 0.0

    target_fields = ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin']
    if all(_value(lead, enriched, field) for field in target_fields):
        meta['__skip_reason'] = 'Openmart company enrich skipped — owner fields already filled'
        return 0.0

    website = _value(lead, enriched, 'website')
    company_linkedin = _value(lead, enriched, 'company_linkedin')
    social_media_link = company_linkedin if _looks_like_company_linkedin(company_linkedin) else None
    if not website and not social_media_link:
        meta['__skip_reason'] = 'Openmart company enrich skipped — no website or company social profile'
        return 0.0

    if _x402_insufficient and not os.getenv('ORTHOGONAL_API_KEY', '').strip():
        meta['__skip_reason'] = 'Openmart company enrich skipped — insufficient x402 balance'
        return 0.0

    from pipeline.openmart import enrich_company
    data = enrich_company(company_website=website, social_media_link=social_media_link)
    result = _extract_openmart_owner_contact(data)
    if not result:
        meta['__skip_reason'] = 'Openmart company enrich returned no owner contact'
        return OPENMART_COST_PER_CALL if data else 0.0

    _merge(enriched, meta, result, 'openmart')
    return OPENMART_COST_PER_CALL


# ── Step 4: Hunter.io — email lookup ───────────────────────────────────────

# Role precedence for Hunter owner selection (higher = better)
HUNTER_ROLE_RANK = [
    ('owner', 100), ('founder', 100), ('co-founder', 100),
    ('president', 80),
    ('ceo', 70), ('chief executive', 70),
    ('principal', 60), ('managing partner', 60), ('partner', 55),
    ('general manager', 50), ('executive director', 50),
    ('vp', 40), ('vice president', 40),
]


def _hunter_role_score(position: str | None) -> int:
    if not position:
        return 0
    p = position.lower()
    best = 0
    for keyword, score in HUNTER_ROLE_RANK:
        if keyword in p and score > best:
            best = score
    return best


def _company_acronym(company: str | None) -> str | None:
    tokens = []
    for token in re.findall(r'[a-z0-9]+', str(company or '').lower()):
        if token in {'the', 'inc', 'llc', 'co', 'company', 'corporation', 'corp', 'of'}:
            continue
        tokens.append(token)
    acronym = ''.join(token[0] for token in tokens if token)
    if 3 <= len(acronym) <= 6:
        return acronym
    return None


def _hunter_domain_candidates(company: str | None, domain: str | None) -> list[str]:
    candidates = []
    if domain:
        candidates.append(domain)
    acronym = _company_acronym(company)
    if acronym:
        candidates.extend([f'{acronym}.com', f'{acronym}.org'])
    out = []
    seen = set()
    for candidate in candidates:
        candidate = candidate.lower().strip()
        if candidate and candidate not in seen:
            seen.add(candidate)
            out.append(candidate)
    return out


def _hunter_email_finder(owner_name: str, domain_candidates: list[str], company: str | None = None, lead_id=None) -> dict:
    result = {}
    for finder_domain in domain_candidates:
        try:
            parts = str(owner_name).split()
            finder_params = {'domain': finder_domain, 'full_name': str(owner_name)}
            if parts:
                finder_params['first_name'] = parts[0]
            if len(parts) > 1:
                finder_params['last_name'] = ' '.join(parts[1:])
            finder_data = _provider_get_json(
                'hunter',
                '/v2/email-finder',
                query=finder_params,
                timeout=SLOW_TIMEOUT,
            )
            finder_data = finder_data.get('data', finder_data) or {}
            first = str(finder_data.get('first_name') or '').strip()
            last = str(finder_data.get('last_name') or '').strip()
            full_name = ' '.join(p for p in (first, last) if p).strip()
            if full_name and not _same_person_name(full_name, owner_name):
                continue
            email = str(finder_data.get('email') or '').strip()
            if email and not _generic_email(email):
                result['owner_email'] = email
            if full_name and _same_person_name(full_name, owner_name):
                result['owner_name'] = full_name
            for key in ('linkedin', 'linkedin_url'):
                if finder_data.get(key) and _looks_like_person_linkedin(finder_data.get(key)):
                    result['owner_linkedin'] = str(finder_data[key]).strip()
                    break
            for key in ('phone_number', 'phone'):
                if finder_data.get(key):
                    result['owner_phone'] = str(finder_data[key]).strip()
                    break
            if result.get('owner_email'):
                break
        except Exception as finder_error:
            _error_logger.error(
                'Hunter email-finder soft failure lead_id=%s company=%s domain=%s owner_name=%s error=%s',
                lead_id, company, finder_domain, owner_name, finder_error,
            )
    return result


def _step_hunter(lead: dict, enriched: dict, meta: dict) -> float:
    if not _paid_provider_available():
        return 0.0
    target_fields = ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin', 'key_staff']
    missing = [f for f in target_fields if not _value(lead, enriched, f)]
    if not missing:
        return 0.0
    domain = _domain_from_website(_value(lead, enriched, 'website'))
    company = _value(lead, enriched, 'company')
    if not domain or not company:
        return 0.0
    domain_candidates = _hunter_domain_candidates(company, domain)

    last_error = None
    for attempt in range(HUNTER_RETRIES + 1):
        try:
            data = {}
            emails = []
            active_domain = domain
            domain_errors = []
            for candidate_domain in domain_candidates:
                try:
                    data = _provider_get_json(
                        'hunter',
                        '/v2/domain-search',
                        query={'domain': candidate_domain, 'limit': 5},
                        timeout=SLOW_TIMEOUT,
                    )
                except Exception as domain_error:
                    _error_logger.error(
                        'Hunter domain-search soft failure lead_id=%s company=%s domain=%s error=%s',
                        lead.get('id'), company, candidate_domain, domain_error,
                    )
                    domain_errors.append(domain_error)
                    continue
                email_data = data.get('data', data) or {}
                emails = email_data.get('emails', []) or []
                active_domain = candidate_domain
                if emails:
                    break
            if not data and domain_errors:
                raise domain_errors[-1]

            def candidate_name(c: dict) -> str | None:
                first = (c.get('first_name') or '').strip()
                last = (c.get('last_name') or '').strip()
                full = ' '.join(part for part in (first, last) if part).strip()
                return full or None

            def candidate_phone(c: dict) -> str | None:
                for key in ('phone_number', 'phone', 'mobile_phone', 'work_phone'):
                    if c.get(key):
                        return str(c[key]).strip()
                return None

            def candidate_linkedin(c: dict) -> str | None:
                for key in ('linkedin', 'linkedin_url'):
                    if c.get(key) and _looks_like_person_linkedin(c.get(key)):
                        return str(c[key]).strip()
                return None

            def is_executive(c: dict) -> bool:
                if (c.get('seniority') or '').lower() == 'executive':
                    return True
                position = (c.get('position') or '').lower()
                return any(k in position for k in
                           ('owner', 'founder', 'president', 'ceo', 'principal',
                            'general manager', 'executive director'))

            # Select owner: executives first, ranked by role then confidence.
            executives = [c for c in emails if is_executive(c)]
            if executives:
                best = max(executives, key=lambda c: (
                    _hunter_role_score(c.get('position')),
                    c.get('confidence') or 0,
                ))
            elif emails:
                best = max(emails, key=lambda c: c.get('confidence') or 0)
            else:
                best = None

            hunter_result = {}
            apollo_search_result = {}
            existing_owner_name = _value(lead, enriched, 'owner_name')
            if best:
                best_name = candidate_name(best)
                best_matches_existing = (
                    not existing_owner_name
                    or (best_name and _same_person_name(best_name, existing_owner_name))
                )
                if best_matches_existing and best.get('value'):
                    hunter_result['owner_email'] = str(best['value']).strip()
                if best_name and not existing_owner_name:
                    hunter_result['owner_name'] = best_name
                best_phone = candidate_phone(best)
                if best_matches_existing and best_phone:
                    hunter_result['owner_phone'] = best_phone
                best_linkedin = candidate_linkedin(best)
                if best_matches_existing and best_linkedin:
                    hunter_result['owner_linkedin'] = best_linkedin

            # key_staff: all returned people as "Name — Position"
            staff = []
            for c in emails:
                name = candidate_name(c)
                position = (c.get('position') or '').strip()
                if name and position:
                    entry = f'{name} — {position}'
                elif name:
                    entry = name
                else:
                    continue
                if entry not in staff:
                    staff.append(entry)
            if staff:
                hunter_result['key_staff'] = staff

            if not (hunter_result.get('owner_name') or _value(lead, enriched, 'owner_name')):
                try:
                    apollo_search_result = _search_apollo_decision_maker(lead, enriched)
                except Exception as search_error:
                    _error_logger.error(
                        'Apollo decision-maker search soft failure lead_id=%s company=%s domain=%s error=%s',
                        lead.get('id'), company, domain, search_error,
                    )

            # email-finder follow-up (useful when owner_name came from scrape)
            owner_name = (
                hunter_result.get('owner_name')
                or apollo_search_result.get('owner_name')
                or _value(lead, enriched, 'owner_name')
            )
            if owner_name and not hunter_result.get('owner_email'):
                finder_domains = [active_domain] + [d for d in domain_candidates if d != active_domain]
                hunter_result.update(_hunter_email_finder(owner_name, finder_domains, company=company, lead_id=lead.get('id')))

            _merge(enriched, meta, apollo_search_result, 'apollo_search')
            _merge(enriched, meta, hunter_result, 'hunter')
            return 0.01
        except requests.exceptions.Timeout as e:
            last_error = e
            if attempt < HUNTER_RETRIES:
                time.sleep(0.5 * (attempt + 1))
                continue
        except Exception as e:
            last_error = e
            break
    raise RuntimeError(f'Hunter: {last_error}')


# ── Step 5: Apollo — people match ──────────────────────────────────────────

def _step_apollo(lead: dict, enriched: dict, meta: dict) -> float:
    if not _paid_provider_available():
        return 0.0
    owner_target_fields = ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin']
    org_target_fields = ['year_established', 'revenue_estimate', 'company_description',
                         'employee_count', 'facebook_url', 'services_offered',
                         'company_phone', 'key_staff']
    target_fields = owner_target_fields + org_target_fields
    missing = [f for f in target_fields if not (enriched.get(f) or lead.get(f))]
    if not missing:
        return 0.0
    owner_name = _value(lead, enriched, 'owner_name') or ''
    domain = _domain_from_website(_value(lead, enriched, 'website'))
    company = _value(lead, enriched, 'company') or ''
    company_linkedin = _value(lead, enriched, 'company_linkedin') or ''
    # Apollo needs enough context to have a realistic match chance.
    owner_email = _value(lead, enriched, 'owner_email') or ''
    if not company or (not owner_name and not domain and not owner_email and not company_linkedin):
        return 0.0
    try:
        payload = {
            'reveal_personal_emails': True,
        }
        if _value(lead, enriched, 'company'):
            payload['organization_name'] = _value(lead, enriched, 'company')
        if domain:
            payload['domain'] = domain
        if owner_email:
            payload['email'] = owner_email
        if _looks_like_company_linkedin(company_linkedin):
            payload['organization_linkedin_url'] = str(company_linkedin).strip()
        owner_linkedin = _value(lead, enriched, 'owner_linkedin')
        if _looks_like_person_linkedin(owner_linkedin):
            payload['linkedin_url'] = str(owner_linkedin).strip()
        if owner_name:
            payload['name'] = owner_name
            parts = owner_name.split(' ')
            if len(parts) > 1:
                payload['first_name'] = parts[0]
                payload['last_name'] = ' '.join(parts[1:])
        data = _provider_post_json(
            'apollo',
            '/api/v1/people/match',
            body=payload,
            timeout=SLOW_TIMEOUT,
        )
        person = data.get('person') or {}
        org = person.get('organization') or {}
        result = {}
        person_name = person.get('name')
        person_matches_request = not owner_name or _same_person_name(str(person_name or ''), owner_name)
        if person_name and person_matches_request:
            result['owner_name'] = person['name']
        if person_matches_request and person.get('email'):
            result['owner_email'] = person['email']
        if person_matches_request and person.get('phone_numbers'):
            phones = person['phone_numbers']
            if phones:
                result['owner_phone'] = phones[0].get('sanitized_number') or phones[0].get('raw_number')
        if person_matches_request and _looks_like_person_linkedin(person.get('linkedin_url')):
            result['owner_linkedin'] = person['linkedin_url']
        # Organization fields
        if org.get('founded_year'):
            try:
                result['year_established'] = int(org['founded_year'])
            except (ValueError, TypeError):
                pass
        if org.get('annual_revenue_printed'):
            result['revenue_estimate'] = str(org['annual_revenue_printed'])
        if org.get('short_description'):
            result['company_description'] = str(org['short_description'])
        if org.get('estimated_num_employees'):
            try:
                result['employee_count'] = int(org['estimated_num_employees'])
            except (ValueError, TypeError):
                pass
        if org.get('facebook_url'):
            result['facebook_url'] = str(org['facebook_url'])
        keywords = org.get('keywords') or []
        if keywords:
            result['services_offered'] = [str(k) for k in keywords[:10]]
        primary_phone = (org.get('primary_phone') or {}).get('sanitized_number')
        if primary_phone:
            result['company_phone'] = str(primary_phone)

        # key_staff from top-level people list
        people_list = data.get('people') or []
        if people_list:
            staff = [p.get('name') for p in people_list[:5] if p.get('name')]
            if staff:
                result['key_staff'] = staff
        _merge(enriched, meta, result, 'apollo')
        return 0.01
    except Exception as e:
        raise RuntimeError(f'Apollo: {e}')


# ── Step 4: Sixtyfour — lead enrichment ────────────────────────────────────

def _extract_sixtyfour_owner_contact(data: dict) -> dict:
    candidates = []
    for record in _walk_dicts(data or {}):
        name = _name_from_record(record)
        email = _first_present(record, ('owner_email', 'email', 'work_email', 'verified_email', 'business_email'))
        phone = _first_present(record, ('owner_phone', 'phone', 'phone_number', 'mobile_phone', 'direct_phone', 'work_phone'))
        linkedin = _first_present(record, ('owner_linkedin', 'linkedin', 'linkedin_url', 'person_linkedin_url'))
        title = _first_present(record, ('title', 'position', 'job_title', 'role'))
        if not any((name, email, phone, linkedin)):
            continue
        role_score = max(_openmart_role_score(title), _apollo_decision_role_score(title))
        if not role_score and not (name and (email or phone or linkedin)):
            continue
        score = role_score
        score += 30 if name else 0
        score += 30 if email and not _generic_email(email) else 0
        score += 25 if phone else 0
        score += 10 if linkedin and _looks_like_person_linkedin(linkedin) else 0
        result = {
            'owner_name': name,
            'owner_email': email,
            'owner_phone': phone,
            'owner_linkedin': linkedin if _looks_like_person_linkedin(linkedin) else None,
        }
        if name and title:
            result['key_staff'] = [f'{name} — {title}']
        elif name:
            result['key_staff'] = [name]
        candidates.append((score, result))
    if not candidates:
        return {}
    candidates.sort(key=lambda item: item[0], reverse=True)
    return {k: v for k, v in candidates[0][1].items() if v}


def _sixtyfour_company_payload(lead: dict, enriched: dict, domain: str | None) -> dict:
    target_company = {
        'name': _value(lead, enriched, 'company'),
        'website': _value(lead, enriched, 'website'),
        'domain': domain,
        'address': _value(lead, enriched, 'address'),
        'city': _value(lead, enriched, 'city'),
        'state': _value(lead, enriched, 'state'),
        'industry': _value(lead, enriched, 'industry'),
        'company_email': _value(lead, enriched, 'company_email'),
        'company_phone': _value(lead, enriched, 'company_phone'),
    }
    return {
        'target_company': {k: v for k, v in target_company.items() if v},
        'find_people': True,
        'struct': {
            'owner_name': 'Full name of the most senior owner, founder, president, CEO, general manager, or executive director.',
            'owner_email': 'Verified business email for that person, if available.',
            'owner_phone': 'Direct phone or mobile phone for that person, if available.',
            'owner_linkedin': 'LinkedIn profile URL for that person, if available.',
            'decision_makers': [
                {
                    'name': 'Full name',
                    'title': 'Role/title',
                    'email': 'Business email',
                    'phone': 'Direct phone',
                    'linkedin_url': 'LinkedIn profile URL',
                }
            ],
        },
        'lead_struct': {
            'name': 'Full name',
            'title': 'Role/title',
            'email': 'Business email',
            'phone': 'Direct phone',
            'linkedin_url': 'LinkedIn profile URL',
        },
        'people_focus_prompt': (
            'Find the most senior decision maker for this organization. Prefer literal owners, founders, '
            'presidents, CEOs, principals, or managing partners. If the organization is a public agency, '
            'utility, district, nonprofit, or municipality with no owner, return the general manager, '
            'executive director, or equivalent top operating executive. Only return contact details that '
            'are explicitly grounded in sources.'
        ),
        'research_plan': (
            'Search the company website, leadership/team pages, public profiles, and reputable business '
            'directories for a senior decision maker and direct business contact data.'
        ),
    }


def _sixtyfour_phone_payload(lead: dict, enriched: dict, domain: str) -> dict:
    owner_name = _value(lead, enriched, 'owner_name')
    owner_email = _value(lead, enriched, 'owner_email')
    owner_linkedin = _value(lead, enriched, 'owner_linkedin')
    company = _value(lead, enriched, 'company')
    location = f"{_value(lead, enriched, 'city') or ''}, {_value(lead, enriched, 'state') or ''}".strip(', ')
    lead_info = {
        'name': str(owner_name),
        'company': str(company),
        'domain': domain,
        'location': location,
    }
    if owner_email:
        lead_info['email'] = str(owner_email)
    if _looks_like_person_linkedin(owner_linkedin):
        lead_info['linkedin_url'] = str(owner_linkedin)

    payload = {
        'lead': lead_info,
        'name': str(owner_name),
        'company': str(company),
        'domain': domain,
    }
    if owner_email:
        payload['email'] = str(owner_email)
    if _looks_like_person_linkedin(owner_linkedin):
        payload['linkedin_url'] = str(owner_linkedin)
    return payload


def _sixtyfour_email_payload(lead: dict, enriched: dict, domain: str) -> dict:
    owner_name = _value(lead, enriched, 'owner_name')
    owner_linkedin = _value(lead, enriched, 'owner_linkedin')
    company = _value(lead, enriched, 'company')
    location = f"{_value(lead, enriched, 'city') or ''}, {_value(lead, enriched, 'state') or ''}".strip(', ')
    payload = {
        'lead': {
            'name': str(owner_name),
            'company': str(company),
            'domain': domain,
            'location': location,
        },
        'name': str(owner_name),
        'company': str(company),
        'domain': domain,
    }
    if _looks_like_person_linkedin(owner_linkedin):
        payload['lead']['linkedin_url'] = str(owner_linkedin)
        payload['linkedin_url'] = str(owner_linkedin)
    return payload


def _extract_sixtyfour_email(data: dict, owner_name: str | None, domain: str | None) -> str | None:
    for record in _walk_dicts(data or {}):
        email = _first_present(record, ('owner_email', 'email', 'work_email', 'verified_email', 'business_email'))
        if not email or _generic_email(email):
            continue
        returned_name = _name_from_record(record)
        if returned_name and owner_name and not _same_person_name(returned_name, owner_name):
            continue
        if _email_matches_domain(email, domain) or record.get('verified') or record.get('confidence'):
            return str(email).strip()
    return None


def _step_owner_email_followup(lead: dict, enriched: dict, meta: dict) -> float:
    if not _paid_provider_available():
        return 0.0
    if _truthful_owner_email_present(lead, enriched, meta):
        meta['__skip_reason'] = 'Owner email follow-up skipped — truthful owner_email already filled'
        return 0.0

    website = _value(lead, enriched, 'website')
    domain = _domain_from_website(website)
    if not domain or not str(website or '').lower().startswith(('http://', 'https://')):
        meta['__skip_reason'] = 'Owner email follow-up skipped — no verified domain'
        return 0.0

    owner_name = _value(lead, enriched, 'owner_name')
    company = _value(lead, enriched, 'company')
    if not owner_name or not company:
        meta['__skip_reason'] = 'Owner email follow-up skipped — no grounded owner or senior decision-maker name'
        return 0.0

    total_cost = 0.0
    result = _hunter_email_finder(
        str(owner_name),
        _hunter_domain_candidates(company, domain),
        company=company,
        lead_id=lead.get('id'),
    )
    total_cost += 0.01
    if result.get('owner_email'):
        _merge(enriched, meta, result, 'hunter')
        return total_cost

    try:
        data = _provider_post_json(
            'sixtyfour',
            '/find-email',
            body=_sixtyfour_email_payload(lead, enriched, domain),
            timeout=SIXTYFOUR_TIMEOUT,
        )
    except Exception as e:
        raise RuntimeError(f'Sixtyfour find-email: {e}')

    total_cost += 0.30
    email = _extract_sixtyfour_email(data or {}, str(owner_name), domain)
    if not email:
        meta['__skip_reason'] = 'Owner email follow-up ran but returned no owner-specific email'
        return total_cost

    _merge(enriched, meta, {'owner_email': email}, 'sixtyfour')
    return total_cost


def _step_sixtyfour(lead: dict, enriched: dict, meta: dict) -> float:
    if not _paid_provider_available():
        return 0.0
    if _value(lead, enriched, 'owner_phone'):
        meta['__skip_reason'] = 'Phone provider skipped — owner_phone already filled'
        return 0.0

    website = _value(lead, enriched, 'website')
    domain = _domain_from_website(website)
    if not domain or not str(website or '').lower().startswith(('http://', 'https://')):
        meta['__skip_reason'] = 'Phone provider skipped — no verified domain'
        return 0.0

    company = _value(lead, enriched, 'company')
    if not company:
        meta['__skip_reason'] = 'Phone provider skipped due to missing company name'
        return 0.0

    total_cost = 0.0
    if not _value(lead, enriched, 'owner_name'):
        try:
            company_data = _provider_post_json(
                'sixtyfour',
                '/enrich-company',
                body=_sixtyfour_company_payload(lead, enriched, domain),
                timeout=SIXTYFOUR_TIMEOUT,
            )
        except Exception as e:
            raise RuntimeError(f'Sixtyfour enrich-company: {e}')
        contact = _extract_sixtyfour_owner_contact(company_data or {})
        if contact:
            _merge(enriched, meta, contact, 'sixtyfour')
        total_cost += 0.10

    owner_name = _value(lead, enriched, 'owner_name')
    if not owner_name:
        meta['__skip_reason'] = 'Phone provider skipped — no owner or senior decision-maker name found'
        return total_cost

    if _value(lead, enriched, 'owner_phone'):
        return total_cost

    try:
        data = _provider_post_json(
            'sixtyfour',
            '/find-phone',
            body=_sixtyfour_phone_payload(lead, enriched, domain),
            timeout=SIXTYFOUR_TIMEOUT,
        )
    except Exception as e:
        raise RuntimeError(f'Sixtyfour find-phone: {e}')

    data = data or {}
    phone = data.get('phone') or data.get('owner_phone')
    if not phone and isinstance(data.get('person'), dict):
        phone = data['person'].get('phone') or data['person'].get('phone_number')
    if not phone:
        meta['__skip_reason'] = 'Phone provider returned no phone'
        return total_cost + 0.30

    _merge(enriched, meta, {'owner_phone': str(phone).strip()}, 'sixtyfour')
    return total_cost + 0.30


# ── Step: FullEnrich — paid fallback for owner contact ────────────────────

def _step_fullenrich(lead: dict, enriched: dict, meta: dict) -> float:
    from pipeline import fullenrich
    if not fullenrich.has_api_key():
        return 0.0
    target_fields = ['owner_name', 'owner_email', 'owner_phone']
    missing = [f for f in target_fields if not _value(lead, enriched, f)]
    if not missing:
        meta['__skip_reason'] = 'FullEnrich skipped — all owner fields already filled'
        return 0.0
    company = _value(lead, enriched, 'company') or ''
    if not company:
        meta['__skip_reason'] = 'FullEnrich skipped — missing company name'
        return 0.0
    try:
        result = fullenrich.enrich_person(
            company=company,
            domain=_domain_from_website(_value(lead, enriched, 'website')),
            owner_name=_value(lead, enriched, 'owner_name'),
            city=_value(lead, enriched, 'city'),
            state=_value(lead, enriched, 'state'),
            linkedin=_value(lead, enriched, 'owner_linkedin'),
        )
    except Exception as e:
        raise RuntimeError(f'FullEnrich: {e}')
    if not result:
        meta['__skip_reason'] = 'FullEnrich returned no new owner data'
        return 0.0
    _merge(enriched, meta, result, 'fullenrich')
    return fullenrich.FULLENRICH_COST_PER_CALL


# ── Step 5: ScrapeGraphAI — website scrape ─────────────────────────────────

def _step_scrape_website(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin',
                     'services_offered', 'year_established',
                     'company_description', 'certifications', 'facebook_url',
                     'yelp_url', 'employee_count', 'company_email', 'company_phone']
    missing = [f for f in target_fields if not (enriched.get(f) or lead.get(f))]
    if not missing:
        return 0.0
    website = _value(lead, enriched, 'website')
    if not website or not has_firecrawl_api_key():
        return 0.0
    try:
        pages = scrape_site_pages(website)
        if not pages:
            return 0.0
        pages = _relevant_pages_for_company(_value(lead, enriched, 'company'), pages)
        evidence = '\n\n'.join(
            f'URL: {page["url"]}\n{page["markdown"][:6000]}'
            for page in pages
        )
        prompt = (
            'Extract company information from the website content below. '
            'Only return values that are explicitly supported by the text. '
            'Ignore content that is unrelated to the company named in the source pages. '
            'Do not guess or infer email addresses or phone numbers. '
            'For owner_name, look in About Us, Team, Leadership, or bio sections — '
            'identify the Owner, Founder, Co-Founder, President, or CEO by name '
            'when the role is explicit. If this is a public agency, utility, district, '
            'municipality, or nonprofit with no literal owner, return the General Manager, '
            'Executive Director, or equivalent top operating executive instead. '
            'For owner_email, owner_phone, and owner_linkedin, only return contact details '
            'that are explicitly attached to that person. If multiple owners or executives, '
            'return the primary one. '
            'If a field is not clearly present, use null. '
            'Return ONLY JSON with keys: '
            'owner_name, owner_email, owner_phone, owner_linkedin, '
            'company_email, company_phone, services_offered, '
            'year_established, company_description, certifications, '
            'facebook_url, yelp_url, employee_count.\n\n'
            f'{evidence}'
        )
        response = _claude_call_with_retry(
            lambda: claude.messages.create(
                model='claude-haiku-4-5-20251001',
                max_tokens=800,
                messages=[{'role': 'user', 'content': prompt}],
            )
        )
        text = response.content[0].text
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()
        result_data = json.loads(text)
        result = {}
        if result_data.get('owner_name'):
            result['owner_name'] = str(result_data['owner_name']).strip()
        if result_data.get('owner_email'):
            result['owner_email'] = str(result_data['owner_email']).strip()
        if result_data.get('owner_phone'):
            result['owner_phone'] = str(result_data['owner_phone']).strip()
        if result_data.get('owner_linkedin') and _looks_like_person_linkedin(result_data.get('owner_linkedin')):
            result['owner_linkedin'] = str(result_data['owner_linkedin']).strip()
        if result_data.get('company_email'):
            result['company_email'] = str(result_data['company_email'])
        if result_data.get('company_phone'):
            result['company_phone'] = str(result_data['company_phone'])
        if result_data.get('services_offered'):
            val = result_data['services_offered']
            result['services_offered'] = val if isinstance(val, list) else [val]
        if result_data.get('year_established'):
            try:
                result['year_established'] = int(result_data['year_established'])
            except (ValueError, TypeError):
                pass
        if result_data.get('company_description'):
            result['company_description'] = str(result_data['company_description'])
        if result_data.get('certifications'):
            val = result_data['certifications']
            result['certifications'] = val if isinstance(val, list) else [val]
        if result_data.get('facebook_url'):
            result['facebook_url'] = str(result_data['facebook_url'])
        if result_data.get('yelp_url'):
            result['yelp_url'] = str(result_data['yelp_url'])
        if result_data.get('employee_count'):
            try:
                result['employee_count'] = int(result_data['employee_count'])
            except (ValueError, TypeError):
                pass
        _merge(enriched, meta, result, 'scrape')
        provider_used = 'direct' if all((page.get('provider_used') == 'direct') for page in pages) else 'direct_then_zyte'
        _tag_fields(meta, list(result.keys()), provider_used)
        cost = (response.usage.input_tokens * 0.003 + response.usage.output_tokens * 0.015) / 1000
        return cost
    except Exception as e:
        raise RuntimeError(f'Website scrape: {e}')


# ── Step 7: Firecrawl — review scrape ──────────────────────────────────────

def _step_scrape_reviews(lead: dict, enriched: dict, meta: dict) -> float:
    if not ENABLE_REVIEW_SCRAPE:
        return 0.0
    if _value(lead, enriched, 'review_summary'):
        return 0.0
    review_url = _value(lead, enriched, 'google_maps_url')
    if not review_url:
        review_url = _value(lead, enriched, 'yelp_url')
    if not review_url or not has_firecrawl_api_key():
        return 0.0
    try:
        page = scrape_url(review_url)
        markdown = (page.get('markdown') or page.get('content') or '')[:8000]
        if not markdown:
            return 0.0
        prompt = (
            'Summarize the top customer review themes from the review page text below. '
            'Return ONLY JSON with key review_summary. If there is not enough review text, use null.\n\n'
            f'{markdown}'
        )
        response = claude.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=250,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = response.content[0].text
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()
        result_data = json.loads(text)
        if result_data.get('review_summary'):
            _merge(enriched, meta, {'review_summary': str(result_data['review_summary'])}, 'scrape')
            _tag_fields(meta, ['review_summary'], page.get('provider_used'))
        cost = (response.usage.input_tokens * 0.003 + response.usage.output_tokens * 0.015) / 1000
        return cost
    except Exception as e:
        raise RuntimeError(f'Review scrape: {e}')


# ── Step 7: Claude failsafe ───────────────────────────────────────────────

# Fields Claude failsafe is allowed to infer (non-contact only)
CLAUDE_FAILSAFE_FIELDS = {
    'employee_count', 'key_staff', 'year_established', 'services_offered',
    'company_description', 'revenue_estimate', 'certifications',
    'review_summary', 'facebook_url', 'yelp_url',
}


def _step_claude_failsafe(lead: dict, enriched: dict, meta: dict) -> float:
    has_evidence = _has_grounded_evidence(lead, enriched, meta)
    # Minimal identity: company name + at least city or address
    has_identity = bool(
        _value(lead, enriched, 'company')
        and (_value(lead, enriched, 'city') or _value(lead, enriched, 'address'))
    )

    if not has_evidence and not has_identity:
        meta['__skip_reason'] = (
            'No grounded API evidence and no company identity (name + city/address) '
            'to anchor inference.'
        )
        return 0.0

    missing = _get_missing(lead, enriched)
    # Only consider fields the failsafe is allowed to fill.
    missing = [f for f in missing if f in CLAUDE_FAILSAFE_FIELDS]
    if not missing:
        meta['__skip_reason'] = 'All allowed failsafe fields are already populated.'
        return 0.0
    if has_evidence and set(missing).issubset(LOW_VALUE_CLAUDE_FIELDS):
        meta['__skip_reason'] = (
            f'Only low-value fields remained ({", ".join(sorted(missing))}); '
            f'skipped to save tokens.'
        )
        return 0.0
    known = {}
    for k in ['company', 'owner_name', 'company_email', 'company_phone', 'address', 'city',
              'state', 'website', 'industry', 'rating', 'review_count',
              'ownership_type']:
        if lead.get(k):
            known[k] = lead[k]
    for k, v in enriched.items():
        if v and k != 'enrichment_meta':
            known[k] = v

    prompt = (
        f'I have the following data about a company:\n\n'
        f'{json.dumps(known, indent=2, default=str)}\n\n'
        f'Infer reasonable values for these missing NON-CONTACT fields: '
        f'{", ".join(missing)}\n\n'
        f'IMPORTANT: You MUST NOT invent or guess owner_name, owner_email, owner_phone, '
        f'or owner_linkedin. Only the fields listed above are allowed. '
        f'For text[] fields (key_staff, services_offered, certifications), return JSON arrays. '
        f'For employee_count and year_established, return integers. '
        f'For all others, return strings. '
        f'If you truly cannot infer a value, use null.\n\n'
        f'Return ONLY a JSON object with the listed field names as keys.'
    )

    try:
        response = _claude_call_with_retry(
            lambda: claude.messages.create(
                model='claude-haiku-4-5-20251001',
                max_tokens=800,
                messages=[{'role': 'user', 'content': prompt}],
            )
        )
        text = response.content[0].text
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()
        inferred = json.loads(text)
        for k, v in inferred.items():
            # Hard guardrail: never write contact fields even if Claude returned them.
            if k not in CLAUDE_FAILSAFE_FIELDS:
                continue
            if k in missing and v is not None:
                enriched[k] = v
                meta[k] = {'source': 'claude_inferred'}
        cost = (response.usage.input_tokens * 0.003 + response.usage.output_tokens * 0.015) / 1000
        return cost
    except Exception as e:
        raise RuntimeError(f'Claude failsafe: {e}')


# ── Main entry point ───────────────────────────────────────────────────────

_STEP_FN_NAMES = {
    'google_places':     '_step_google_places',
    'google_maps':       '_step_google_maps',
    'domain_recovery':   '_step_domain_recovery',
    'openmart_company':  '_step_openmart_company',
    'hunter':            '_step_hunter',
    'apollo':            '_step_apollo',
    'owner_email_followup': '_step_owner_email_followup',
    'sixtyfour':         '_step_sixtyfour',
    'fullenrich':        '_step_fullenrich',
    'scrape_website':    '_step_scrape_website',
    'scrape_reviews':    '_step_scrape_reviews',
    'company_fallback':  '_step_company_contact_fallback',
    'claude_failsafe':   '_step_claude_failsafe',
}

STAGE_1 = ['google_places', 'google_maps']
STAGE_2 = ['domain_recovery']
STAGE_3 = ['scrape_website']
STAGE_4 = ['openmart_company', 'apollo', 'hunter']
STAGE_5 = ['sixtyfour']
STAGE_6 = ['owner_email_followup', 'fullenrich']
STAGE_7 = ['scrape_reviews', 'company_fallback', 'claude_failsafe']

# Backward compat alias so existing imports still work.
STEPS = [
    (k, globals()[_STEP_FN_NAMES[k]])
    for k in STAGE_1 + STAGE_2 + STAGE_3 + STAGE_4 + STAGE_5 + STAGE_6 + STAGE_7
]


def _get_step_fn(step_key: str):
    """Look up step function at call time so patches take effect."""
    import pipeline.enrichment as mod
    fn_name = _STEP_FN_NAMES[step_key]
    return getattr(mod, fn_name)


def _run_step(step_key, lead, enriched, meta, emit, company):
    """Run a single enrichment step, emit events, return cost."""
    step_fn = _get_step_fn(step_key)
    step_name = STEP_NAMES[step_key]
    emit({
        'type': 'enrich_step_start',
        'lead_id': lead.get('id'),
        'company': company,
        'step': step_name,
    })
    keys_before = set(enriched.keys())
    was_insufficient = _x402_insufficient
    start = time.time()
    try:
        cost = step_fn(lead, enriched, meta)
        elapsed = time.time() - start
        new_keys = set(enriched.keys()) - keys_before
        fields_filled = [k for k in new_keys if k != 'enrichment_meta']
        field_values = {}
        field_sources = {}
        for k in fields_filled:
            v = enriched[k]
            if isinstance(v, list):
                field_values[k] = ', '.join(str(x) for x in v)
            else:
                field_values[k] = str(v) if v is not None else None
            field_sources[k] = (meta.get(k) or {}).get('provider') or (meta.get(k) or {}).get('source')
        event_type = 'enrich_step_done'
        detail = None
        dynamic_reason = meta.pop('__skip_reason', None)
        if not fields_filled and cost == 0.0:
            event_type = 'enrich_step_skip'
            if _x402_insufficient and not _has_orthogonal_api_key() and step_key in ('hunter', 'apollo', 'sixtyfour'):
                detail = 'Skipped — insufficient x402 balance'
            elif dynamic_reason:
                detail = dynamic_reason
            else:
                detail = STEP_SKIP_REASONS.get(step_key, 'Step ran but produced no new fields.')
        elif not fields_filled:
            detail = dynamic_reason or 'Step ran but produced no new fields.'
        emit({
            'type': event_type,
            'lead_id': lead.get('id'),
            'company': company,
            'step': step_name,
            'cost': cost,
            'elapsed': round(elapsed, 1),
            'fields_filled': fields_filled,
            'field_values': field_values,
            'field_sources': field_sources,
            'detail': detail,
        })
        return cost
    except Exception as e:
        elapsed = time.time() - start
        _error_logger.error(
            'lead_id=%s company=%s step=%s error=%s\n%s',
            lead.get('id'), company, step_name, e, traceback.format_exc(),
        )
        emit({
            'type': 'enrich_step_error',
            'lead_id': lead.get('id'),
            'company': company,
            'step': step_name,
            'error': str(e),
            'elapsed': round(elapsed, 1),
        })
        # Emit insufficient_funds only on the FIRST 402 (flag just flipped)
        if _x402_insufficient and not was_insufficient and not _has_orthogonal_api_key():
            balance = check_x402_balance()
            emit({
                'type': 'insufficient_funds',
                'message': f"get ur money up — x402 payment failed. Balance: ${balance:.2f}",
            })
        return 0.0


def _merge_stage4_result(enriched, meta, step_enriched, step_meta):
    """Merge an owner-identity subresult into the main dicts using source priority."""
    for field, value in step_enriched.items():
        if _is_empty(value):
            continue
        source_info = step_meta.get(field) or {}
        source = source_info.get('source') or source_info.get('provider')
        if _should_replace(field, enriched.get(field), meta, value, source):
            enriched[field] = value
            meta[field] = dict(source_info)
            continue
        if field not in enriched or _is_empty(enriched[field]):
            enriched[field] = value
            meta[field] = dict(source_info)


def enrich_lead(lead_id: int, emit=None, wait_if_paused=None) -> dict:
    """
    Run the enrichment waterfall in 7 stages:
      Stage 1 (sequential): Google Places -> Google Maps URL
      Stage 2 (sequential): Domain recovery
      Stage 3 (sequential): Website scrape
      Stage 4 (parallel):   Apollo | Hunter
      Stage 5 (sequential): Sixtyfour phone lookup
      Stage 6 (sequential): FullEnrich fallback
      Stage 7 (sequential): Review scrape -> Company fallback -> Claude failsafe
    Returns {'cost': float, 'sources': dict}.
    """
    if emit is None:
        emit = lambda e: None

    def _pause_check():
        if wait_if_paused is not None:
            wait_if_paused()

    lead = get_lead(lead_id)
    if not lead:
        return {'cost': 0.0, 'sources': {}}

    company = lead.get('company', f'lead #{lead_id}')
    enriched: dict = {}
    meta: dict = {}
    total_cost = 0.0

    # Stage 1: business identity (sequential)
    for step_key in STAGE_1:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Stage 2: recover missing website/domain before scrape and owner lookups
    for step_key in STAGE_2:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Stage 3: company evidence (sequential so owner_name can feed Stage 4)
    for step_key in STAGE_3:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Stage 4: owner identity/contact (parallel, merged by source priority)
    _pause_check()
    from pipeline.config import ENRICH_PHASE2_CONCURRENCY

    stage4_results = {}
    merged_lead = {**lead, **enriched}

    def run_stage4_step(step_key):
        step_enriched = {}
        step_meta = {}
        cost = _run_step(step_key, merged_lead, step_enriched, step_meta, emit, company)
        return step_key, cost, step_enriched, step_meta

    with ThreadPoolExecutor(max_workers=ENRICH_PHASE2_CONCURRENCY) as executor:
        futures = {
            executor.submit(run_stage4_step, step_key): step_key
            for step_key in STAGE_4
        }
        for future in as_completed(futures):
            try:
                step_key, cost, step_enriched, step_meta = future.result()
                stage4_results[step_key] = (cost, step_enriched, step_meta)
            except Exception as exc:
                _error_logger.error(
                    'lead_id=%s company=%s stage4_future step=%s error=%s\n%s',
                    lead_id, company, futures[future], exc, traceback.format_exc(),
                )

    # Merge in deterministic order; field-level priority decides replacements.
    for step_key in STAGE_4:
        if step_key in stage4_results:
            cost, step_enriched, step_meta = stage4_results[step_key]
            total_cost += cost
            _merge_stage4_result(enriched, meta, step_enriched, step_meta)

    # Stage 5: owner phone provider (sequential)
    for step_key in STAGE_5:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Stage 6: final paid owner fallback (sequential)
    for step_key in STAGE_6:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Stage 7: finalization (sequential)
    for step_key in STAGE_7:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Persist
    if enriched:
        enriched['enrichment_meta'] = json.dumps(meta)
        update_lead(lead_id, enriched)

    return {'cost': total_cost, 'sources': meta}
