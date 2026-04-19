"""
Waterfall enrichment — 7 steps per lead.

Uses Orthogonal APIs (via x402) for steps 1-6, Claude API as failsafe (step 7).
Each step only populates fields that are still empty after previous steps.
"""

import logging
import os
import json
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
    'claude_discovery': 'Claude discovery',
    'google_places': 'Google Places',
    'google_maps': 'Google Maps URL',
    'hunter': 'Hunter.io',
    'apollo': 'Apollo',
    'sixtyfour': 'Sixtyfour',
    'scrape_website': 'Website scrape',
    'scrape_reviews': 'Review scrape',
    'company_fallback': 'Company fallback',
    'claude_failsafe': 'Claude failsafe',
}

STEP_SKIP_REASONS = {
    'claude_discovery': 'No company context available to infer website, or website already exists.',
    'google_places': 'No company/address data available for place matching, or no place match found.',
    'google_maps': 'No real Google place ID available.',
    'hunter': 'No website/domain available for owner-contact lookup.',
    'apollo': 'No usable owner/company context remained for people matching.',
    'scrape_website': 'No website available to scrape, or no scraper fallback is configured.',
    'scrape_reviews': 'No review URL available to scrape, or no scraper fallback is configured.',
    'company_fallback': 'No company email or phone available to reuse.',
    'claude_failsafe': 'No missing fields remained to infer.',
}

LOW_VALUE_CLAUDE_FIELDS = {
    'review_summary',
    'facebook_url',
    'yelp_url',
}
HUNTER_RETRIES = 2

OWNER_FIELD_SOURCE_PRIORITY = {
    'owner_name': {
        'apollo': 100,
        'hunter': 80,
        'fullenrich': 60,
        'scrape': 50,
        'company_fallback': 10,
        'claude_inferred': 0,
    },
    'owner_email': {
        'apollo': 100,
        'hunter': 90,
        'fullenrich': 70,
        'company_fallback': 10,
        'claude_inferred': 0,
    },
    'owner_phone': {
        'apollo': 100,
        'hunter': 85,
        'fullenrich': 70,
        'company_fallback': 10,
        'claude_inferred': 0,
    },
    'owner_linkedin': {
        'apollo': 100,
        'hunter': 70,
        'fullenrich': 60,
        'claude_inferred': 0,
    },
    'key_staff': {
        'apollo': 100,
        'hunter': 60,
        'fullenrich': 40,
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


def _generic_email(email: str | None) -> bool:
    if not email or '@' not in email:
        return True
    local = email.split('@', 1)[0].lower()
    return local in GENERIC_EMAIL_PREFIXES


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
    use company-level contact fields so the table and outreach pipeline do not stay blank.
    """
    fallback = {}
    company_email = _value(lead, enriched, 'company_email')
    company_phone = _value(lead, enriched, 'company_phone')
    if not _value(lead, enriched, 'owner_email') and company_email:
        fallback['owner_email'] = company_email
    if not _value(lead, enriched, 'owner_phone') and company_phone:
        fallback['owner_phone'] = company_phone
    if fallback:
        for field, value in fallback.items():
            enriched[field] = value
            meta[field] = {'source': 'company_fallback', 'fallback': True}
    return 0.0


# ── Step 1: Claude discovery ────────────────────────────────────────────────

def _step_claude_discovery(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['website']
    missing = [f for f in target_fields if not _value(lead, enriched, f)]
    if not missing:
        return 0.0

    known = {}
    for key in ['company', 'address', 'city', 'state', 'industry', 'owner_linkedin',
                'company_email', 'company_phone', 'website']:
        value = _value(lead, enriched, key)
        if value:
            known[key] = value
    if not known.get('company'):
        return 0.0

    prompt = (
        'You are preparing lookup fields for downstream enrichment of a small/medium business. '
        'Use the known business context below to infer a likely website. '
        'Do not invent contact data (emails, phones). If you cannot determine a field, return null.\n\n'
        f'{json.dumps(known, indent=2, default=str)}\n\n'
        'Return ONLY JSON with key: website.'
    )

    try:
        response = _claude_call_with_retry(
            lambda: claude.messages.create(
                model='claude-haiku-4-5-20251001',
                max_tokens=200,
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
        if result_data.get('website') and not _value(lead, enriched, 'website'):
            result['website'] = str(result_data['website']).strip()
        _merge(enriched, meta, result, 'claude_discovery')
        cost = (response.usage.input_tokens * 0.003 + response.usage.output_tokens * 0.015) / 1000
        return cost
    except Exception as e:
        raise RuntimeError(f'Claude discovery: {e}')


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
        not str(place_id).startswith('CSV_') and
        not _value(lead, enriched, 'google_maps_url')
    ):
        enriched['google_maps_url'] = f'https://www.google.com/maps/place/?q=place_id:{place_id}'
        meta['google_maps_url'] = {'source': 'constructed'}
    return 0.0


# ── Step 4: Hunter.io — email lookup ───────────────────────────────────────

# Role precedence for Hunter owner selection (higher = better)
HUNTER_ROLE_RANK = [
    ('owner', 100), ('founder', 100), ('co-founder', 100),
    ('president', 80),
    ('ceo', 70), ('chief executive', 70),
    ('principal', 60), ('managing partner', 60), ('partner', 55),
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


def _step_hunter(lead: dict, enriched: dict, meta: dict) -> float:
    global _x402_insufficient, _x402_consecutive_402s
    if _x402_insufficient:
        return 0.0
    target_fields = ['owner_name', 'owner_email', 'owner_phone', 'owner_linkedin', 'key_staff']
    missing = [f for f in target_fields if not _value(lead, enriched, f)]
    if not missing:
        return 0.0
    domain = _domain_from_website(_value(lead, enriched, 'website'))
    company = _value(lead, enriched, 'company')
    if not domain or not company:
        return 0.0

    last_error = None
    for attempt in range(HUNTER_RETRIES + 1):
        try:
            session = _x402_session()
            resp = session.get(
                f'{ORTH_BASE}/hunter/v2/domain-search',
                params={'domain': domain, 'limit': 5},
                timeout=SLOW_TIMEOUT,
            )
            if resp.status_code == 402:
                _x402_consecutive_402s += 1
                if _x402_consecutive_402s >= _X402_CONSECUTIVE_THRESHOLD:
                    _x402_insufficient = True
            else:
                _x402_consecutive_402s = 0
            resp.raise_for_status()
            data = resp.json()
            email_data = data.get('data', {}) or {}
            emails = email_data.get('emails', []) or []

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
                    if c.get(key):
                        return str(c[key]).strip()
                return None

            def is_executive(c: dict) -> bool:
                if (c.get('seniority') or '').lower() == 'executive':
                    return True
                position = (c.get('position') or '').lower()
                return any(k in position for k in
                           ('owner', 'founder', 'president', 'ceo', 'principal'))

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

            result = {}
            if best:
                if best.get('value'):
                    result['owner_email'] = str(best['value']).strip()
                best_name = candidate_name(best)
                if best_name:
                    result['owner_name'] = best_name
                best_phone = candidate_phone(best)
                if best_phone:
                    result['owner_phone'] = best_phone
                best_linkedin = candidate_linkedin(best)
                if best_linkedin:
                    result['owner_linkedin'] = best_linkedin

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
                result['key_staff'] = staff

            # email-finder follow-up (useful when owner_name came from scrape)
            owner_name = result.get('owner_name') or _value(lead, enriched, 'owner_name')
            if owner_name:
                try:
                    parts = str(owner_name).split()
                    finder_params = {'domain': domain, 'full_name': str(owner_name)}
                    if parts:
                        finder_params['first_name'] = parts[0]
                    if len(parts) > 1:
                        finder_params['last_name'] = ' '.join(parts[1:])
                    finder_resp = session.get(
                        f'{ORTH_BASE}/hunter/v2/email-finder',
                        params=finder_params,
                        timeout=SLOW_TIMEOUT,
                    )
                    if finder_resp.status_code == 402:
                        _x402_consecutive_402s += 1
                        if _x402_consecutive_402s >= _X402_CONSECUTIVE_THRESHOLD:
                            _x402_insufficient = True
                    else:
                        _x402_consecutive_402s = 0
                    finder_resp.raise_for_status()
                    finder_data = finder_resp.json().get('data', {}) or {}
                    if finder_data.get('email'):
                        result['owner_email'] = str(finder_data['email']).strip()
                    first = str(finder_data.get('first_name') or '').strip()
                    last = str(finder_data.get('last_name') or '').strip()
                    full_name = ' '.join(p for p in (first, last) if p).strip()
                    if full_name:
                        result['owner_name'] = full_name
                    for key in ('linkedin', 'linkedin_url'):
                        if finder_data.get(key):
                            result['owner_linkedin'] = str(finder_data[key]).strip()
                            break
                    for key in ('phone_number', 'phone'):
                        if finder_data.get(key):
                            result['owner_phone'] = str(finder_data[key]).strip()
                            break
                except Exception as finder_error:
                    _error_logger.error(
                        'Hunter email-finder soft failure lead_id=%s company=%s domain=%s owner_name=%s error=%s',
                        lead.get('id'), company, domain, owner_name, finder_error,
                    )

            _merge(enriched, meta, result, 'hunter')
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
    global _x402_insufficient, _x402_consecutive_402s
    if _x402_insufficient:
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
    # Apollo needs enough context to have a realistic match chance.
    owner_email = _value(lead, enriched, 'owner_email') or ''
    if not company or (not owner_name and not domain and not owner_email):
        return 0.0
    try:
        payload = {
            'reveal_personal_emails': True,
            'reveal_phone_number': True,
            'run_waterfall_email': True,
            'run_waterfall_phone': True,
        }
        if _value(lead, enriched, 'company'):
            payload['organization_name'] = _value(lead, enriched, 'company')
        if domain:
            payload['domain'] = domain
        if owner_email:
            payload['email'] = owner_email
        if owner_name:
            payload['name'] = owner_name
            parts = owner_name.split(' ')
            payload['first_name'] = parts[0]
            if len(parts) > 1:
                payload['last_name'] = ' '.join(parts[1:])
        session = _x402_session()
        resp = session.post(
            f'{ORTH_BASE}/apollo/api/v1/people/match',
            json=payload,
                timeout=SLOW_TIMEOUT,
            )
        if resp.status_code == 402:
            _x402_consecutive_402s += 1
            if _x402_consecutive_402s >= _X402_CONSECUTIVE_THRESHOLD:
                _x402_insufficient = True
        else:
            _x402_consecutive_402s = 0
        resp.raise_for_status()
        data = resp.json()
        person = data.get('person') or {}
        org = person.get('organization') or {}
        result = {}
        if person.get('name'):
            result['owner_name'] = person['name']
        if person.get('email'):
            result['owner_email'] = person['email']
        if person.get('phone_numbers'):
            phones = person['phone_numbers']
            if phones:
                result['owner_phone'] = phones[0].get('sanitized_number') or phones[0].get('raw_number')
        if person.get('linkedin_url'):
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

def _step_sixtyfour(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['owner_email', 'owner_phone',
                     'employee_count', 'revenue_estimate']
    missing = [f for f in target_fields if not (enriched.get(f) or lead.get(f))]
    if not missing:
        return 0.0
    cost = 0.0
    domain = _domain_from_website(lead.get('website'))

    # enrich-lead
    try:
        with _x402_session() as session:
            resp = session.post(
                f'{ORTH_BASE}/sixtyfour/enrich-lead',
                json={
                    'lead_info': {
                        'name': lead.get('owner_name') or '',
                        'company': lead.get('company') or '',
                        'location': f"{lead.get('city', '')}, {lead.get('state', '')}",
                        'domain': domain or '',
                    },
                    'struct': {
                        'title': 'Job title or role',
                        'employee_count': 'Number of employees',
                        'revenue': 'Estimated annual revenue',
                    },
                },
                timeout=SLOW_TIMEOUT,
            )
        resp.raise_for_status()
        data = resp.json()
        cost += 0.10
        result = {}
        if data.get('employee_count'):
            try:
                result['employee_count'] = int(data['employee_count'])
            except (ValueError, TypeError):
                pass
        if data.get('revenue'):
            result['revenue_estimate'] = str(data['revenue'])
        _merge(enriched, meta, result, 'sixtyfour')
    except requests.exceptions.Timeout:
        raise RuntimeError('Sixtyfour enrich-lead: timed out (60s)')
    except Exception as e:
        raise RuntimeError(f'Sixtyfour enrich-lead: {e}')

    # find-email if still missing
    if not (enriched.get('owner_email') or lead.get('owner_email')):
        try:
            with _x402_session() as session:
                resp = session.post(
                    f'{ORTH_BASE}/sixtyfour/find-email',
                    json={
                        'lead': {
                            'name': lead.get('owner_name') or '',
                            'company': lead.get('company') or '',
                            'domain': domain or '',
                        },
                        'mode': 'PROFESSIONAL',
                    },
                    timeout=SLOW_TIMEOUT,
                )
            resp.raise_for_status()
            data = resp.json()
            if data.get('email'):
                _merge(enriched, meta, {'owner_email': data['email']}, 'sixtyfour')
        except requests.exceptions.Timeout:
            raise RuntimeError('Sixtyfour find-email: timed out (60s)')
        except Exception as e:
            raise RuntimeError(f'Sixtyfour find-email: {e}')

    # find-phone if still missing
    if not (enriched.get('owner_phone') or lead.get('owner_phone')):
        try:
            with _x402_session() as session:
                resp = session.post(
                    f'{ORTH_BASE}/sixtyfour/find-phone',
                    json={
                        'lead': {
                            'name': lead.get('owner_name') or '',
                            'company': lead.get('company') or '',
                            'domain': domain or '',
                        },
                    },
                    timeout=SLOW_TIMEOUT,
                )
            resp.raise_for_status()
            data = resp.json()
            if data.get('phone'):
                _merge(enriched, meta, {'owner_phone': data['phone']}, 'sixtyfour')
        except requests.exceptions.Timeout:
            raise RuntimeError('Sixtyfour find-phone: timed out (60s)')
        except Exception as e:
            raise RuntimeError(f'Sixtyfour find-phone: {e}')

    return cost


# ── Step 5: ScrapeGraphAI — website scrape ─────────────────────────────────

def _step_scrape_website(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['owner_name', 'services_offered', 'year_established',
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
        evidence = '\n\n'.join(
            f'URL: {page["url"]}\n{page["markdown"][:6000]}'
            for page in pages
        )
        prompt = (
            'Extract company information from the website content below. '
            'Only return values that are explicitly supported by the text. '
            'Do not guess or infer email addresses or phone numbers. '
            'For owner_name, look in About Us, Team, Leadership, or bio sections — '
            'identify the Owner, Founder, Co-Founder, President, or CEO by name '
            'when the role is explicit. If multiple owners, return the primary one. '
            'If a field is not clearly present, use null. '
            'Return ONLY JSON with keys: '
            'owner_name, company_email, company_phone, services_offered, '
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
    missing = _get_missing(lead, enriched)
    # Only consider fields the failsafe is allowed to fill.
    missing = [f for f in missing if f in CLAUDE_FAILSAFE_FIELDS]
    if not missing:
        return 0.0
    if set(missing).issubset(LOW_VALUE_CLAUDE_FIELDS):
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
    'claude_discovery':  '_step_claude_discovery',
    'google_places':     '_step_google_places',
    'google_maps':       '_step_google_maps',
    'hunter':            '_step_hunter',
    'apollo':            '_step_apollo',
    'scrape_website':    '_step_scrape_website',
    'scrape_reviews':    '_step_scrape_reviews',
    'company_fallback':  '_step_company_contact_fallback',
    'claude_failsafe':   '_step_claude_failsafe',
}

PHASE_1 = ['claude_discovery', 'google_places', 'google_maps']
PHASE_2 = ['hunter', 'scrape_website']
PHASE_3 = ['apollo', 'scrape_reviews', 'company_fallback', 'claude_failsafe']

# Backward compat
STEPS = [(k, globals()[_STEP_FN_NAMES[k]]) for k in PHASE_1 + PHASE_2 + PHASE_3]


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
        if not fields_filled and cost == 0.0:
            event_type = 'enrich_step_skip'
            if _x402_insufficient and step_key in ('hunter', 'apollo'):
                detail = 'Skipped — insufficient x402 balance'
            else:
                detail = STEP_SKIP_REASONS.get(step_key, 'Step ran but produced no new fields.')
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
        if _x402_insufficient and not was_insufficient:
            balance = check_x402_balance()
            emit({
                'type': 'insufficient_funds',
                'message': f"get ur money up — x402 payment failed. Balance: ${balance:.2f}",
            })
        return 0.0


def enrich_lead(lead_id: int, emit=None, wait_if_paused=None) -> dict:
    """
    Run the enrichment waterfall in 3 phases:
      Phase 1 (sequential): Claude discovery -> Google Places -> Google Maps URL
      Phase 2 (parallel):   Hunter | Website scrape
      Phase 3 (sequential): Apollo -> Review scrape -> Company fallback -> Claude failsafe
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

    # Phase 1: sequential foundation
    for step_key in PHASE_1:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Phase 2: parallel data fetch
    _pause_check()
    from pipeline.config import ENRICH_PHASE2_CONCURRENCY

    phase2_results = {}

    def run_phase2_step(step_key):
        step_enriched = {}
        step_meta = {}
        # Snapshot lead + phase1 enriched so phase2 steps see phase1 results
        merged_lead = {**lead, **enriched}
        cost = _run_step(step_key, merged_lead, step_enriched, step_meta, emit, company)
        return step_key, cost, step_enriched, step_meta

    with ThreadPoolExecutor(max_workers=ENRICH_PHASE2_CONCURRENCY) as executor:
        futures = {
            executor.submit(run_phase2_step, step_key): step_key
            for step_key in PHASE_2
        }
        for future in as_completed(futures):
            try:
                step_key, cost, step_enriched, step_meta = future.result()
                phase2_results[step_key] = (cost, step_enriched, step_meta)
            except Exception as exc:
                _error_logger.error(
                    'lead_id=%s company=%s phase2_future step=%s error=%s\n%s',
                    lead_id, company, futures[future], exc, traceback.format_exc(),
                )

    # Merge phase 2 results in deterministic order (hunter -> scrape)
    for step_key in PHASE_2:
        if step_key in phase2_results:
            cost, step_enriched, step_meta = phase2_results[step_key]
            total_cost += cost
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

    # Phase 3: sequential finalization
    for step_key in PHASE_3:
        _pause_check()
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Write to DB
    if enriched:
        enriched['enrichment_meta'] = json.dumps(meta)
        update_lead(lead_id, enriched)

    return {'cost': total_cost, 'sources': meta}
