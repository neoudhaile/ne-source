"""
Waterfall enrichment — 7 steps per lead.

Uses Orthogonal APIs (via x402) for steps 1-6, Claude API as failsafe (step 7).
Each step only populates fields that are still empty after previous steps.
"""

import os
import json
import requests
import anthropic
from eth_account import Account
from x402 import x402ClientSync
from x402.mechanisms.evm.exact.v1.client import ExactEvmSchemeV1
from x402.http.clients.requests import x402_http_adapter
from dotenv import load_dotenv

from pipeline.db import get_lead, update_lead

load_dotenv()

# ── x402 session (same pattern as scraper.py) ──────────────────────────────
account = Account.from_key(os.getenv('PRIVATE_KEY'))
_x402 = x402ClientSync()
_x402.register_v1('base', ExactEvmSchemeV1(signer=account))
session = requests.Session()
session.mount('https://', x402_http_adapter(_x402))

# ── Claude client (for failsafe) ───────────────────────────────────────────
claude = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

ENRICHABLE_FIELDS = [
    'owner_email', 'owner_phone', 'owner_linkedin', 'owner_title',
    'employee_count', 'key_staff', 'year_established', 'services_offered',
    'company_description', 'revenue_estimate', 'certifications',
    'review_summary', 'facebook_url', 'yelp_url', 'google_maps_url',
]

ORTH_BASE = 'https://x402.orth.sh'


# ── Helpers ─────────────────────────────────────────────────────────────────

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


def _merge(enriched: dict, meta: dict, result: dict, source: str):
    """Merge non-empty values from result into enriched, tag in meta."""
    for k, v in result.items():
        if v is None or v == '' or v == []:
            continue
        if k not in enriched or enriched[k] is None or enriched[k] == '' or enriched[k] == []:
            enriched[k] = v
            meta[k] = {'source': source}


# ── Step 1: Construct google_maps_url (free) ───────────────────────────────

def _step_google_maps(lead: dict, enriched: dict, meta: dict) -> float:
    place_id = lead.get('google_place_id')
    if place_id and not (enriched.get('google_maps_url') or lead.get('google_maps_url')):
        enriched['google_maps_url'] = f'https://www.google.com/maps/place/?q=place_id:{place_id}'
        meta['google_maps_url'] = {'source': 'constructed'}
    return 0.0


# ── Step 2: Hunter.io — email lookup ───────────────────────────────────────

def _step_hunter(lead: dict, enriched: dict, meta: dict) -> float:
    if enriched.get('owner_email') or lead.get('owner_email'):
        return 0.0
    domain = _domain_from_website(lead.get('website'))
    if not domain:
        return 0.0
    try:
        # GET with query params, not POST with JSON
        resp = session.get(
            f'{ORTH_BASE}/hunter/v2/domain-search',
            params={'domain': domain, 'limit': 5},
        )
        resp.raise_for_status()
        data = resp.json()
        emails = data.get('data', {}).get('emails', [])
        if emails:
            _merge(enriched, meta, {'owner_email': emails[0].get('value')}, 'hunter')
        return 0.01
    except Exception as e:
        print(f'  Hunter error: {e}')
        return 0.01


# ── Step 3: Apollo — people match ──────────────────────────────────────────

def _step_apollo(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['owner_email', 'owner_phone', 'owner_linkedin',
                     'owner_title', 'employee_count', 'key_staff']
    missing = [f for f in target_fields if not (enriched.get(f) or lead.get(f))]
    if not missing:
        return 0.0
    try:
        payload = {
            'first_name': (lead.get('owner_name') or '').split(' ')[0],
            'last_name': ' '.join((lead.get('owner_name') or '').split(' ')[1:]),
            'organization_name': lead.get('company'),
            'domain': _domain_from_website(lead.get('website')),
            'reveal_personal_emails': True,
            'reveal_phone_number': True,
        }
        resp = session.post(
            f'{ORTH_BASE}/apollo/api/v1/people/match',
            json=payload,
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
        if person.get('title'):
            result['owner_title'] = person['title']
        if org.get('estimated_num_employees'):
            result['employee_count'] = org['estimated_num_employees']
        # key_staff from employment_history or related people
        people_list = data.get('people') or []
        if people_list:
            staff = [p.get('name') for p in people_list[:5] if p.get('name')]
            if staff:
                result['key_staff'] = staff
        _merge(enriched, meta, result, 'apollo')
        return 0.01
    except Exception as e:
        print(f'  Apollo error: {e}')
        return 0.01


# ── Step 4: Sixtyfour — lead enrichment ────────────────────────────────────

def _step_sixtyfour(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['owner_email', 'owner_phone', 'owner_title',
                     'employee_count', 'revenue_estimate']
    missing = [f for f in target_fields if not (enriched.get(f) or lead.get(f))]
    if not missing:
        return 0.0
    cost = 0.0
    domain = _domain_from_website(lead.get('website'))
    try:
        # enrich-lead: requires lead_info + struct objects
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
        )
        resp.raise_for_status()
        data = resp.json()
        cost += 0.10
        result = {}
        if data.get('title'):
            result['owner_title'] = data['title']
        if data.get('employee_count'):
            try:
                result['employee_count'] = int(data['employee_count'])
            except (ValueError, TypeError):
                pass
        if data.get('revenue'):
            result['revenue_estimate'] = str(data['revenue'])
        _merge(enriched, meta, result, 'sixtyfour')
    except Exception as e:
        print(f'  Sixtyfour enrich error: {e}')
        cost += 0.10

    # find-email if still missing — requires lead object
    if not (enriched.get('owner_email') or lead.get('owner_email')):
        try:
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
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get('email'):
                _merge(enriched, meta, {'owner_email': data['email']}, 'sixtyfour')
        except Exception as e:
            print(f'  Sixtyfour find-email error: {e}')

    # find-phone if still missing — requires lead object
    if not (enriched.get('owner_phone') or lead.get('owner_phone')):
        try:
            resp = session.post(
                f'{ORTH_BASE}/sixtyfour/find-phone',
                json={
                    'lead': {
                        'name': lead.get('owner_name') or '',
                        'company': lead.get('company') or '',
                        'domain': domain or '',
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get('phone'):
                _merge(enriched, meta, {'owner_phone': data['phone']}, 'sixtyfour')
        except Exception as e:
            print(f'  Sixtyfour find-phone error: {e}')

    return cost


# ── Step 5: ScrapeGraphAI — website scrape ─────────────────────────────────

def _step_scrape_website(lead: dict, enriched: dict, meta: dict) -> float:
    target_fields = ['services_offered', 'year_established', 'company_description',
                     'certifications', 'facebook_url', 'yelp_url', 'employee_count']
    missing = [f for f in target_fields if not (enriched.get(f) or lead.get(f))]
    if not missing:
        return 0.0
    website = lead.get('website')
    if not website:
        return 0.0
    try:
        prompt = (
            'Extract the following from this company website: '
            '1) services offered (list), '
            '2) year established, '
            '3) company description (1-2 sentences), '
            '4) certifications or licenses (list), '
            '5) Facebook page URL, '
            '6) Yelp page URL, '
            '7) number of employees. '
            'Return as JSON with keys: services_offered, year_established, '
            'company_description, certifications, facebook_url, yelp_url, employee_count.'
        )
        resp = session.post(
            f'{ORTH_BASE}/scrapegraph/v1/smartscraper',
            json={
                'user_prompt': prompt,
                'website_url': website,
                'output_schema': {
                    'properties': {
                        'services_offered': {'type': 'array', 'items': {'type': 'string'}},
                        'year_established': {'type': 'string'},
                        'company_description': {'type': 'string'},
                        'certifications': {'type': 'array', 'items': {'type': 'string'}},
                        'facebook_url': {'type': 'string'},
                        'yelp_url': {'type': 'string'},
                        'employee_count': {'type': 'string'},
                    },
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()
        # smartscraper returns the result in 'result' key or directly
        result_data = data.get('result') or data
        if isinstance(result_data, str):
            result_data = json.loads(result_data)
        result = {}
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
        _merge(enriched, meta, result, 'scrapegraphai')
        return 0.04
    except Exception as e:
        print(f'  ScrapeGraphAI website error: {e}')
        return 0.04


# ── Step 6: ScrapeGraphAI — review scrape ──────────────────────────────────

def _step_scrape_reviews(lead: dict, enriched: dict, meta: dict) -> float:
    if enriched.get('review_summary') or lead.get('review_summary'):
        return 0.0
    # Use google maps URL or yelp URL
    review_url = enriched.get('google_maps_url') or lead.get('google_maps_url')
    if not review_url:
        review_url = enriched.get('yelp_url') or lead.get('yelp_url')
    if not review_url:
        return 0.0
    try:
        prompt = (
            'Extract the top review themes and customer sentiment from this page. '
            'Summarize in 2-3 sentences what customers love and any common complaints. '
            'Return as JSON with key: review_summary.'
        )
        resp = session.post(
            f'{ORTH_BASE}/scrapegraph/v1/smartscraper',
            json={
                'user_prompt': prompt,
                'website_url': review_url,
                'output_schema': {
                    'properties': {
                        'review_summary': {'type': 'string'},
                    },
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()
        result_data = data.get('result') or data
        if isinstance(result_data, str):
            result_data = json.loads(result_data)
        if result_data.get('review_summary'):
            _merge(enriched, meta, {'review_summary': str(result_data['review_summary'])}, 'scrapegraphai')
        return 0.04
    except Exception as e:
        print(f'  ScrapeGraphAI reviews error: {e}')
        return 0.04


# ── Step 7: Claude failsafe ───────────────────────────────────────────────

def _step_claude_failsafe(lead: dict, enriched: dict, meta: dict) -> float:
    missing = _get_missing(lead, enriched)
    if not missing:
        return 0.0
    # Build context from all known data
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
            max_tokens=500,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = response.content[0].text
        # Extract JSON from response
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
        print(f'  Claude failsafe error: {e}')
        return 0.005


# ── Main entry point ───────────────────────────────────────────────────────

def enrich_lead(lead_id: int, emit=None) -> dict:
    """
    Run the full 7-step waterfall on one lead.
    Returns {'cost': float, 'sources': dict}.
    """
    lead = get_lead(lead_id)
    if not lead:
        return {'cost': 0.0, 'sources': {}}

    enriched: dict = {}
    meta: dict = {}
    total_cost = 0.0

    total_cost += _step_google_maps(lead, enriched, meta)
    total_cost += _step_hunter(lead, enriched, meta)
    total_cost += _step_apollo(lead, enriched, meta)
    total_cost += _step_sixtyfour(lead, enriched, meta)
    total_cost += _step_scrape_website(lead, enriched, meta)
    total_cost += _step_scrape_reviews(lead, enriched, meta)
    total_cost += _step_claude_failsafe(lead, enriched, meta)

    # Write to DB
    if enriched:
        enriched['enrichment_meta'] = json.dumps(meta)
        update_lead(lead_id, enriched)

    return {'cost': total_cost, 'sources': meta}
