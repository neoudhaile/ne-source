"""
Waterfall enrichment — 7 steps per lead.

Uses Orthogonal APIs (via x402) for steps 1-6, Claude API as failsafe (step 7).
Each step only populates fields that are still empty after previous steps.
"""

import os
import json
import threading
import time
import requests
import anthropic
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

# ── Claude client (for failsafe) ───────────────────────────────────────────
claude = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

ENRICHABLE_FIELDS = [
    'owner_email', 'owner_phone', 'owner_linkedin',
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
    'claude_discovery': 'No company context available to infer lookup fields, or lookup fields already exist.',
    'google_places': 'No company/address data available for place matching, or no place match found.',
    'google_maps': 'No real Google place ID available.',
    'hunter': 'No website/domain available for email lookup.',
    'apollo': 'No owner name or website/domain available for people matching.',
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


def _merge(enriched: dict, meta: dict, result: dict, source: str):
    """Merge non-empty values from result into enriched, tag in meta."""
    for k, v in result.items():
        if v is None or v == '' or v == []:
            continue
        if k not in enriched or enriched[k] is None or enriched[k] == '' or enriched[k] == []:
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
    company_email = _value(lead, enriched, 'email')
    company_phone = _value(lead, enriched, 'phone')
    if not _value(lead, enriched, 'owner_email') and company_email:
        fallback['owner_email'] = company_email
    if not _value(lead, enriched, 'owner_phone') and company_phone:
        fallback['owner_phone'] = company_phone
    _merge(enriched, meta, fallback, 'company_fallback')
    return 0.0


# ── Step 1: Claude discovery ────────────────────────────────────────────────

def _step_claude_discovery(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['website', 'owner_name']
    missing = [f for f in target_fields if not _value(lead, enriched, f)]
    if not missing:
        return 0.0

    known = {}
    for key in ['company', 'address', 'city', 'state', 'industry', 'owner_linkedin', 'email', 'phone', 'website']:
        value = _value(lead, enriched, key)
        if value:
            known[key] = value
    if not known.get('company'):
        return 0.0

    prompt = (
        'You are preparing lookup fields for downstream enrichment. '
        'Use the known business context below to infer likely values ONLY for website and owner_name. '
        'Be conservative. If a field is not reasonably supportable, return null. '
        'Do not invent personal contact data.\n\n'
        f'{json.dumps(known, indent=2, default=str)}\n\n'
        'Return ONLY JSON with keys: website, owner_name.'
    )

    try:
        response = claude.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=200,
            messages=[{'role': 'user', 'content': prompt}],
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
        if result_data.get('owner_name') and not _value(lead, enriched, 'owner_name'):
            result['owner_name'] = str(result_data['owner_name']).strip()
        _merge(enriched, meta, result, 'claude_discovery')
        cost = (response.usage.input_tokens * 0.003 + response.usage.output_tokens * 0.015) / 1000
        return cost
    except Exception as e:
        raise RuntimeError(f'Claude discovery: {e}')


# ── Step 2: Google Places details/match ─────────────────────────────────────

def _step_google_places(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['website', 'phone', 'google_maps_url', 'rating', 'review_count']
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
        'phone': details.get('nationalPhoneNumber'),
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

def _step_hunter(lead: dict, enriched: dict, meta: dict) -> float:
    if _value(lead, enriched, 'owner_email'):
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
            resp.raise_for_status()
            data = resp.json()
            emails = data.get('data', {}).get('emails', [])
            if emails:
                _merge(enriched, meta, {'owner_email': emails[0].get('value')}, 'hunter')
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
    target_fields = ['owner_email', 'owner_phone', 'owner_linkedin',
                     'employee_count', 'key_staff']
    missing = [f for f in target_fields if not (enriched.get(f) or lead.get(f))]
    if not missing:
        return 0.0
    owner_name = _value(lead, enriched, 'owner_name') or ''
    domain = _domain_from_website(_value(lead, enriched, 'website'))
    company = _value(lead, enriched, 'company') or ''
    # Apollo needs enough context to have a realistic match chance.
    if not company or (not owner_name and not domain):
        return 0.0
    try:
        payload = {
            'reveal_personal_emails': True,
        }
        if _value(lead, enriched, 'company'):
            payload['organization_name'] = _value(lead, enriched, 'company')
        if domain:
            payload['domain'] = domain
        if owner_name:
            payload['first_name'] = owner_name.split(' ')[0]
            last = ' '.join(owner_name.split(' ')[1:])
            if last:
                payload['last_name'] = last
        session = _x402_session()
        resp = session.post(
            f'{ORTH_BASE}/apollo/api/v1/people/match',
            json=payload,
            timeout=FAST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        person = data.get('person') or {}
        org = person.get('organization') or {}
        result = {}
        if person.get('email'):
            result['owner_email'] = person['email']
        if person.get('phone_numbers'):
            phones = person['phone_numbers']
            if phones:
                result['owner_phone'] = phones[0].get('sanitized_number') or phones[0].get('raw_number')
        if person.get('linkedin_url'):
            result['owner_linkedin'] = person['linkedin_url']
        if org.get('estimated_num_employees'):
            result['employee_count'] = org['estimated_num_employees']
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
    target_fields = ['services_offered', 'year_established', 'company_description',
                     'certifications', 'facebook_url', 'yelp_url', 'employee_count']
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
            'If a field is not clearly present, use null. '
            'Return ONLY JSON with keys: '
            'email, phone, owner_name, services_offered, year_established, '
            'company_description, certifications, facebook_url, yelp_url, employee_count.\n\n'
            f'{evidence}'
        )
        response = claude.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=800,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = response.content[0].text
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()
        result_data = json.loads(text)
        result = {}
        if result_data.get('email'):
            result['email'] = str(result_data['email'])
        if result_data.get('phone'):
            result['phone'] = str(result_data['phone'])
        if result_data.get('owner_name'):
            result['owner_name'] = str(result_data['owner_name'])
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

def _step_claude_failsafe(lead: dict, enriched: dict, meta: dict) -> float:
    missing = _get_missing(lead, enriched)
    if not missing:
        return 0.0
    if set(missing).issubset(LOW_VALUE_CLAUDE_FIELDS):
        return 0.0
    known = {}
    for k in ['company', 'owner_name', 'email', 'phone', 'address', 'city',
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
        f'I am missing these fields: {", ".join(missing)}\n\n'
        f'Based on the company name, location, industry, website, and any other '
        f'context above, infer reasonable values for the missing fields. '
        f'For text[] fields (key_staff, services_offered, certifications), return JSON arrays. '
        f'For employee_count and year_established, return integers. '
        f'For all others, return strings. '
        f'If you truly cannot infer a value, use null.\n\n'
        f'Return ONLY a JSON object with the missing field names as keys.'
    )

    try:
        response = claude.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=800,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = response.content[0].text
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()
        inferred = json.loads(text)
        for k, v in inferred.items():
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
PHASE_2 = ['hunter', 'apollo', 'scrape_website']
PHASE_3 = ['scrape_reviews', 'company_fallback', 'claude_failsafe']

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
        emit({
            'type': 'enrich_step_error',
            'lead_id': lead.get('id'),
            'company': company,
            'step': step_name,
            'error': str(e),
            'elapsed': round(elapsed, 1),
        })
        return 0.0


def enrich_lead(lead_id: int, emit=None) -> dict:
    """
    Run the enrichment waterfall in 3 phases:
      Phase 1 (sequential): Claude discovery -> Google Places -> Google Maps URL
      Phase 2 (parallel):   Hunter | Apollo | Firecrawl website
      Phase 3 (sequential): Review scrape -> Company fallback -> Claude failsafe
    Returns {'cost': float, 'sources': dict}.
    """
    if emit is None:
        emit = lambda e: None

    lead = get_lead(lead_id)
    if not lead:
        return {'cost': 0.0, 'sources': {}}

    company = lead.get('company', f'lead #{lead_id}')
    enriched: dict = {}
    meta: dict = {}
    total_cost = 0.0

    # Phase 1: sequential foundation
    for step_key in PHASE_1:
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Phase 2: parallel data fetch
    from concurrent.futures import ThreadPoolExecutor, as_completed
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
            except Exception:
                pass

    # Merge phase 2 results in deterministic order (hunter -> apollo -> scrape)
    for step_key in PHASE_2:
        if step_key in phase2_results:
            cost, step_enriched, step_meta = phase2_results[step_key]
            total_cost += cost
            for field, value in step_enriched.items():
                if value is None or value == '' or value == []:
                    continue
                if field not in enriched or enriched[field] is None or enriched[field] == '' or enriched[field] == []:
                    enriched[field] = value
            for field, source_info in step_meta.items():
                if field in enriched and enriched[field] == step_enriched.get(field):
                    meta[field] = source_info

    # Phase 3: sequential finalization
    for step_key in PHASE_3:
        total_cost += _run_step(step_key, lead, enriched, meta, emit, company)

    # Write to DB
    if enriched:
        enriched['enrichment_meta'] = json.dumps(meta)
        update_lead(lead_id, enriched)

    return {'cost': total_cost, 'sources': meta}
