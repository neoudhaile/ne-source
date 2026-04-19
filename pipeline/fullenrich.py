"""FullEnrich API client — paid owner-contact fallback.

Activates automatically once FULLENRICH_API_KEY is set in .env. The public
endpoint path and request shape below follow FullEnrich's documented
/api/v1/enrich/person route. If their API shape turns out to differ, only
this file needs to change.
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

FULLENRICH_BASE = os.getenv('FULLENRICH_BASE_URL', 'https://api.fullenrich.com')
FULLENRICH_COST_PER_CALL = 0.10  # estimated; adjust when real pricing is known
FULLENRICH_TIMEOUT = 30


def has_api_key() -> bool:
    return bool(os.getenv('FULLENRICH_API_KEY'))


def enrich_person(
    company: str,
    domain: str | None,
    owner_name: str | None,
    city: str | None,
    state: str | None,
    linkedin: str | None,
) -> dict:
    """Call FullEnrich and return a dict with any of:
    owner_name, owner_email, owner_phone, owner_linkedin.
    Returns {} on any error or empty response."""
    api_key = os.getenv('FULLENRICH_API_KEY')
    if not api_key:
        return {}

    payload = {
        'company': company or '',
        'domain': domain or '',
        'full_name': owner_name or '',
        'city': city or '',
        'state': state or '',
        'linkedin_url': linkedin or '',
    }
    payload = {k: v for k, v in payload.items() if v}

    try:
        resp = requests.post(
            f'{FULLENRICH_BASE}/api/v1/enrich/person',
            json=payload,
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=FULLENRICH_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json() or {}
    except Exception:
        return {}

    result = {}
    person = data.get('person') or data
    if person.get('full_name') or person.get('name'):
        result['owner_name'] = str(person.get('full_name') or person.get('name')).strip()
    if person.get('email'):
        result['owner_email'] = str(person['email']).strip()
    if person.get('phone'):
        result['owner_phone'] = str(person['phone']).strip()
    if person.get('linkedin_url') or person.get('linkedin'):
        result['owner_linkedin'] = str(person.get('linkedin_url') or person.get('linkedin')).strip()
    return result
