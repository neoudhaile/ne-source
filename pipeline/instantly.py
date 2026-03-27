import os
import requests
from dotenv import load_dotenv

load_dotenv()

INSTANTLY_API_KEY     = os.getenv('INSTANTLY_API_KEY', '')
INSTANTLY_CAMPAIGN_ID = os.getenv('INSTANTLY_CAMPAIGN_ID', '')

BASE_URL = 'https://api.instantly.ai/api/v2'


def _lead_to_instantly(lead: dict) -> dict:
    """Map a smb_leads dict to an Instantly lead payload."""
    full_name = (lead.get('owner_name') or '').strip()
    parts     = full_name.split(' ', 1)
    first     = parts[0] if parts else ''
    last      = parts[1] if len(parts) > 1 else ''

    return {
        'email':        lead.get('owner_email') or lead.get('email') or '',
        'first_name':   first,
        'last_name':    last,
        'company_name': lead.get('company') or '',
        'phone':        lead.get('phone') or '',
        'website':      lead.get('website') or '',
        'custom_variables': {
            'industry':       lead.get('industry') or '',
            'city':           lead.get('city') or '',
            'state':          lead.get('state') or '',
            'address':        lead.get('address') or '',
            'ownership_type': lead.get('ownership_type') or '',
            'distance_miles': str(lead.get('distance_miles') or ''),
            'rating':         str(lead.get('rating') or ''),
            'review_count':   str(lead.get('review_count') or ''),
            'subject':        lead.get('generated_subject') or '',
            'email_body':     lead.get('generated_email') or '',
        },
    }


def push_leads(leads: list[dict]) -> dict:
    """
    Push a batch of leads to the configured Instantly campaign.
    Returns {'pushed': N, 'skipped': N, 'failed': N}.
    Raises ValueError if API key or campaign ID is not configured.
    """
    if not INSTANTLY_API_KEY:
        raise ValueError('INSTANTLY_API_KEY is not set in .env')
    if not INSTANTLY_CAMPAIGN_ID:
        raise ValueError('INSTANTLY_CAMPAIGN_ID is not set in .env')

    # Only push leads that have an email — Instantly requires it
    with_email    = [l for l in leads if l.get('owner_email') or l.get('email')]
    without_email = len(leads) - len(with_email)

    if not with_email:
        return {'pushed': 0, 'skipped': without_email, 'failed': 0}

    payload = {
        'campaign_id': INSTANTLY_CAMPAIGN_ID,
        'leads':       [_lead_to_instantly(l) for l in with_email],
    }

    headers = {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type':  'application/json',
    }

    resp = requests.post(f'{BASE_URL}/leads', json=payload, headers=headers, timeout=30)
    resp.raise_for_status()

    return {
        'pushed':  len(with_email),
        'skipped': without_email,
        'failed':  0,
    }
