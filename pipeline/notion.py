"""Export enriched leads to a Notion database via the REST API."""

import json
import os

import requests
from dotenv import load_dotenv

from pipeline.config import ENABLE_NOTION_EXPORT, NOTION_DATABASE_ID
from pipeline.db import get_leads_by_ids

load_dotenv()

NOTION_API_URL = 'https://api.notion.com/v1/pages'
NOTION_VERSION = '2022-06-28'
NOTION_TIMEOUT_SECONDS = 15
VALID_TIERS = {'tier_1', 'tier_2', 'tier_3'}


def _truncate(value, limit: int = 2000) -> str:
    text = str(value or '').strip()
    return text[:limit]


def _notion_headers() -> dict:
    """Return auth headers for the Notion API."""
    token = os.getenv('NOTION_API_KEY', '').strip()
    return {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_VERSION,
    }


def is_viable_lead(lead: dict) -> bool:
    """
    Determine if a lead should be exported to Notion.
    Currently: any lead with non-empty enrichment data.
    """
    meta = lead.get('enrichment_meta')
    if meta is None:
        return False
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (json.JSONDecodeError, TypeError):
            return False
    return bool(meta)


def _rich_text(value) -> dict:
    """Build a Notion rich_text property value."""
    return {'rich_text': [{'text': {'content': _truncate(value)}}]}


def _join_values(value) -> str:
    if isinstance(value, list):
        return ', '.join(str(item) for item in value if item not in (None, ''))
    return str(value)


def _industry_select_name(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value).strip().lower()
    if normalized == 'car wash':
        return 'Car Wash'
    if 'insurance' in normalized:
        return 'Insurance'
    return 'Other'


def _parse_meta(meta) -> dict:
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str):
        try:
            parsed = json.loads(meta)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


OWNER_SOURCE_FIELDS = ('owner_name', 'owner_email', 'owner_phone', 'owner_linkedin')
NON_TRUTHFUL_OWNER_EMAIL_SOURCES = {'company_fallback', 'claude_inferred', 'csv_import'}


def _owner_sources_text(meta: dict) -> str:
    parts = []
    for field in OWNER_SOURCE_FIELDS:
        entry = meta.get(field) or {}
        if not isinstance(entry, dict):
            continue
        source = entry.get('provider') or entry.get('source')
        if not source:
            continue
        label = field.replace('owner_', '')
        parts.append(f'{label}: {source}')
    return ' · '.join(parts)


def _notes_text(lead: dict, meta: dict) -> str:
    notes = []
    email_meta = meta.get('owner_email') or {}
    email_source = (email_meta.get('source') if isinstance(email_meta, dict) else None) or ''
    if lead.get('owner_email') and email_source in NON_TRUTHFUL_OWNER_EMAIL_SOURCES:
        notes.append(f'owner_email is {email_source} (company-level, not verified owner email)')
    if not lead.get('owner_name'):
        notes.append('owner_name missing')
    if not lead.get('owner_email'):
        notes.append('owner_email missing')
    if not lead.get('owner_phone'):
        notes.append('owner_phone missing')
    return '; '.join(notes)


def _lead_to_notion_properties(lead: dict) -> dict:
    """Map a lead dict to Notion database page properties."""
    company = _truncate(lead.get('company') or 'Unknown')
    props = {
        'Company': {'title': [{'text': {'content': company}}]},
    }

    meta = _parse_meta(lead.get('enrichment_meta'))

    text_map = {
        'owner_name': 'Owner Name',
        'city': 'City',
        'state': 'State',
        'revenue_estimate': 'Revenue Estimate',
        'company_description': 'Company Description',
        'tier_reason': 'Tier Reason',
        'source': 'Source',
    }
    for db_field, notion_field in text_map.items():
        value = lead.get(db_field)
        if value not in (None, '', []):
            props[notion_field] = _rich_text(_join_values(value))

    services = lead.get('services_offered')
    if services not in (None, '', []):
        props['Services'] = _rich_text(_join_values(services))

    owner_email = lead.get('owner_email')
    if owner_email:
        props['Owner Email'] = {'email': str(owner_email)}

    owner_phone = lead.get('owner_phone')
    if owner_phone:
        props['Owner Phone'] = {'phone_number': str(owner_phone)}

    company_email = lead.get('company_email')
    if company_email:
        props['Company Email'] = {'email': str(company_email)}

    company_phone = lead.get('company_phone')
    if company_phone:
        props['Company Phone'] = {'phone_number': str(company_phone)}

    owner_sources = _owner_sources_text(meta)
    if owner_sources:
        props['Owner Sources'] = _rich_text(owner_sources)

    notes = _notes_text(lead, meta)
    if notes:
        props['Notes'] = _rich_text(notes)

    url_map = {
        'website': 'Website',
        'owner_linkedin': 'Owner LinkedIn',
        'google_maps_url': 'Google Maps',
    }
    for db_field, notion_field in url_map.items():
        value = lead.get(db_field)
        if value:
            props[notion_field] = {'url': str(value)}

    industry_name = _industry_select_name(lead.get('industry'))
    if industry_name:
        props['Industry'] = {'select': {'name': industry_name}}

    tier = lead.get('tier')
    if tier in VALID_TIERS:
        props['Tier'] = {'select': {'name': tier}}

    number_map = {
        'rating': 'Rating',
        'review_count': 'Review Count',
        'employee_count': 'Employee Count',
        'year_established': 'Year Established',
    }
    for db_field, notion_field in number_map.items():
        value = lead.get(db_field)
        if value is None:
            continue
        try:
            props[notion_field] = {'number': float(value)}
        except (ValueError, TypeError):
            continue

    return props


def _post_to_notion(properties: dict) -> dict:
    """POST a single page to the Notion database. Returns response JSON."""
    payload = {
        'parent': {'database_id': NOTION_DATABASE_ID},
        'properties': properties,
    }
    response = requests.post(
        NOTION_API_URL,
        headers=_notion_headers(),
        json=payload,
        timeout=NOTION_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def export_leads_to_notion(lead_ids: list[int], emit=None) -> dict:
    """Export viable leads to Notion. Returns export counts."""
    if emit is None:
        emit = lambda e: None

    if not ENABLE_NOTION_EXPORT:
        emit({'type': 'export_skip', 'reason': 'Notion export disabled'})
        return {'exported': 0, 'skipped': len(lead_ids), 'errors': 0}

    notion_api_key = os.getenv('NOTION_API_KEY', '').strip()
    if not NOTION_DATABASE_ID.strip() or not notion_api_key:
        emit({'type': 'export_skip', 'reason': 'Notion export not configured'})
        return {'exported': 0, 'skipped': len(lead_ids), 'errors': 0}

    leads = get_leads_by_ids(lead_ids)
    lead_map = {lead['id']: lead for lead in leads}

    exported = 0
    skipped = 0
    errors = 0

    emit({'type': 'export_start', 'count': len(leads)})

    for lead_id in lead_ids:
        lead = lead_map.get(lead_id)
        if not lead:
            skipped += 1
            continue

        if not is_viable_lead(lead):
            skipped += 1
            continue

        try:
            properties = _lead_to_notion_properties(lead)
            _post_to_notion(properties)
            exported += 1
            emit({
                'type': 'export_lead',
                'index': exported,
                'total': len(leads),
                'lead_id': lead_id,
                'company': lead.get('company', ''),
            })
        except Exception as exc:
            errors += 1
            emit({
                'type': 'export_error',
                'lead_id': lead_id,
                'company': lead.get('company', ''),
                'error': str(exc),
                'message': str(exc),
            })

    emit({
        'type': 'export_done',
        'exported': exported,
        'skipped': skipped,
        'errors': errors,
    })

    return {'exported': exported, 'skipped': skipped, 'errors': errors}
