"""
Claude-based lead tiering that runs after sourcing and before enrichment.
"""

import json
import os
import re
from collections import Counter

import anthropic
from dotenv import load_dotenv

from pipeline.db import (
    delete_leads,
    ensure_tiering_columns,
    get_connection,
    get_leads_by_ids,
    update_lead_tiers,
)

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

TIER_HARD_REMOVE = 'hard_remove'
TIER_1 = 'tier_1'
TIER_2 = 'tier_2'
TIER_3 = 'tier_3'

VALID_DECISIONS = {TIER_HARD_REMOVE, TIER_1, TIER_2, TIER_3}
CLAUDE_MODEL = 'claude-haiku-4-5-20251001'
TIER_BATCH_SIZE = 8
CLAUDE_MAX_TOKENS = 1800
CLAUDE_RETRIES = 2

PRIMARY_PATTERNS = [
    'auto repair', 'auto service', 'diesel', 'fleet maintenance', 'fleet service',
    'equipment repair', 'industrial maintenance', 'field service',
    'pest control', 'remediation', 'abatement',
    'industrial distribution', 'industrial supply', 'mro', 'maintenance repair operations',
    'fabrication', 'fabricator', 'machine shop', 'metal works', 'welding',
]
SECONDARY_PATTERNS = [
    'insurance brokerage', 'insurance broker', 'insurance agency',
    'general distribution', 'distributor', 'wholesale',
]
OPERATIONS_PATTERNS = [
    'repair', 'service', 'maintenance', 'fabrication', 'manufacturing',
    'shop', 'field service', 'remediation', 'abatement', 'pest', 'supply',
]
HARD_REMOVE_PATTERNS = [
    'auto body', 'body shop', 'collision', 'paint shop', 'auto paint',
    'refinishing', 'underwriter', 'underwriting', 'insurance carrier',
    'consumer goods distribution', 'food distribution', 'food distributor',
]
CPA_PATTERNS = ['cpa', 'accounting', 'tax service', 'bookkeeping', 'bookkeeper']
SMALL_BIZ_CPA_PATTERNS = [
    'small business', 'bookkeeping', 'tax service', 'quickbooks',
    'payroll', 'business services', 'bookkeeper', 'tax prep',
]


def tier_label(decision: str | None) -> str | None:
    return {
        TIER_HARD_REMOVE: 'Hard Remove',
        TIER_1: 'Tier 1',
        TIER_2: 'Tier 2',
        TIER_3: 'Tier 3',
    }.get(decision or '')


def _fmt(value):
    if value is None or value == '' or value == []:
        return None
    if isinstance(value, list):
        return ', '.join(str(v) for v in value)
    return str(value)


def _lead_payload(lead: dict) -> dict:
    return {
        'lead_id': lead.get('id'),
        'company': _fmt(lead.get('company')),
        'industry': _fmt(lead.get('industry')),
        'company_email': _fmt(lead.get('company_email')),
        'company_phone': _fmt(lead.get('company_phone')),
        'website': _fmt(lead.get('website')),
        'address': _fmt(lead.get('address')),
        'city': _fmt(lead.get('city')),
        'state': _fmt(lead.get('state')),
        'ownership_type': _fmt(lead.get('ownership_type')),
        'owner_linkedin': _fmt(lead.get('owner_linkedin')),
        'employee_count': _fmt(lead.get('employee_count')),
        'revenue_estimate': _fmt(lead.get('revenue_estimate')),
        'services_offered': _fmt(lead.get('services_offered')),
        'company_description': _fmt(lead.get('company_description')),
        'certifications': _fmt(lead.get('certifications')),
        'review_summary': _fmt(lead.get('review_summary')),
        'raw_data_present': lead.get('raw_data') is not None,
    }


def _build_prompt(leads: list[dict]) -> str:
    return f"""Classify each lead into one of: hard_remove, tier_1, tier_2, tier_3.

Rules:
- hard_remove: auto body/collision/paint/refinishing; insurance carriers/underwriters; pure engineering or consulting firms with no physical operations; consumer goods or food distribution
- tier_1: revenue $5-20M and primary vertical; or CPA that appears small-business focused
- tier_2: revenue $1-5M and primary vertical; revenue $5-20M and secondary vertical; or CPA not clearly small-business focused
- tier_3: revenue $1-5M and secondary vertical; plausible but unclear fit; anything not clearly tier_1/tier_2 and not hard_remove

Primary verticals:
- general auto repair, industrial field service/maintenance, equipment repair, environmental services (pest, remediation, abatement), industrial distribution, specialty manufacturing/fabrication

Secondary verticals:
- insurance brokerage, insurance agency, general distribution

Important:
- use only the provided data
- be conservative
- if unsure and not clearly hard_remove, choose tier_3
- keep reasons short and concrete
- return ONLY a JSON array with one object per lead

Input:
{json.dumps([_lead_payload(lead) for lead in leads], indent=2)}

Output:
[{{"lead_id": 123, "decision": "tier_2", "reason": "Short reason."}}]"""


def _parse_response(text: str) -> list[dict]:
    payload = text.strip()
    if '```' in payload:
        payload = payload.split('```')[1]
        if payload.startswith('json'):
            payload = payload[4:]
        payload = payload.strip()
    start = payload.find('[')
    end = payload.rfind(']')
    if start == -1 or end == -1 or end < start:
        raise ValueError('Tiering response did not contain a JSON array')
    payload = payload[start:end + 1]
    parsed = json.loads(payload)
    if not isinstance(parsed, list):
        raise ValueError('Tiering response was not a JSON array')
    return parsed


def _fallback_batch(leads: list[dict], reason: str) -> list[dict]:
    return [
        {
            'lead_id': lead.get('id'),
            'decision': TIER_3,
            'reason': reason,
        }
        for lead in leads
    ]


def _combined_text(lead: dict) -> str:
    parts = [
        _fmt(lead.get('company')),
        _fmt(lead.get('industry')),
        _fmt(lead.get('website')),
        _fmt(lead.get('services_offered')),
        _fmt(lead.get('company_description')),
        _fmt(lead.get('certifications')),
        _fmt(lead.get('review_summary')),
    ]
    return ' | '.join(part for part in parts if part).lower()


def _has_any(text: str, patterns: list[str]) -> bool:
    return any(pattern in text for pattern in patterns)


def _is_cpa(lead: dict, text: str) -> bool:
    industry = (lead.get('industry') or '').lower()
    return 'account' in industry or _has_any(text, CPA_PATTERNS)


def _revenue_bucket(lead: dict) -> str | None:
    value = (lead.get('revenue_estimate') or '').lower().replace(' ', '')
    if not value:
        return None
    if any(token in value for token in ['$5m-$20m', '5m-20m', '5mto20m', '$5m-20m']):
        return '5_20'
    if any(token in value for token in ['$1m-$5m', '1m-5m', '1mto5m', '$1m-5m']):
        return '1_5'
    return None


def _deterministic_rule(lead: dict) -> dict | None:
    text = _combined_text(lead)
    company = lead.get('company') or 'Lead'
    revenue = _revenue_bucket(lead)
    industry = (lead.get('industry') or '').lower()
    has_operations = _has_any(text, OPERATIONS_PATTERNS)
    is_primary = _has_any(text, PRIMARY_PATTERNS)
    is_secondary = _has_any(text, SECONDARY_PATTERNS)
    is_cpa = _is_cpa(lead, text)

    if _has_any(text, HARD_REMOVE_PATTERNS):
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_HARD_REMOVE,
            'reason': 'Clear hard remove based on business type keywords.',
            'decision_source': 'rules',
        }

    if 'insurance' in industry and _has_any(text, ['carrier', 'underwriter', 'underwriting']):
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_HARD_REMOVE,
            'reason': 'Insurance carrier/underwriter is a hard remove.',
            'decision_source': 'rules',
        }

    if (('engineering' in industry or 'consult' in industry) and not has_operations):
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_HARD_REMOVE,
            'reason': 'Pure engineering/consulting firm with no clear operations footprint.',
            'decision_source': 'rules',
        }

    if is_cpa:
        if _has_any(text, SMALL_BIZ_CPA_PATTERNS):
            return {
                'lead_id': lead.get('id'),
                'decision': TIER_1,
                'reason': 'CPA with small-business focus; Tier 1 deal-flow outreach.',
                'decision_source': 'rules',
            }
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_2,
            'reason': 'CPA not clearly small-business focused.',
            'decision_source': 'rules',
        }

    if revenue == '5_20' and is_primary:
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_1,
            'reason': 'Primary vertical and $5-20M revenue fit Tier 1.',
            'decision_source': 'rules',
        }
    if revenue == '1_5' and is_primary:
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_2,
            'reason': 'Primary vertical and $1-5M revenue fit Tier 2.',
            'decision_source': 'rules',
        }
    if revenue == '5_20' and is_secondary:
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_2,
            'reason': 'Secondary vertical and $5-20M revenue fit Tier 2.',
            'decision_source': 'rules',
        }
    if revenue == '1_5' and is_secondary:
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_3,
            'reason': 'Secondary vertical and $1-5M revenue fit Tier 3.',
            'decision_source': 'rules',
        }

    if company and any(term in text for term in ['collision', 'body shop', 'refinish']):
        return {
            'lead_id': lead.get('id'),
            'decision': TIER_HARD_REMOVE,
            'reason': 'Collision/body shop style business is a hard remove.',
            'decision_source': 'rules',
        }

    return None


def _normalize_items(items: list[dict], leads: list[dict]) -> list[dict]:
    by_id = {lead.get('id'): lead for lead in leads}
    normalized = []
    seen = set()
    for item in items:
        lead_id = item.get('lead_id')
        decision = str(item.get('decision') or '').strip().lower()
        reason = str(item.get('reason') or '').strip()
        if lead_id in seen or lead_id not in by_id or decision not in VALID_DECISIONS:
            continue
        seen.add(lead_id)
        normalized.append({
            'lead_id': lead_id,
            'decision': decision,
            'reason': reason or 'Tiered from available company profile.',
            'decision_source': 'claude',
        })
    return normalized


def _fallback_reason(kind: str) -> str:
    return {
        'api_error': 'Claude tiering API error; defaulted to Tier 3.',
        'timeout': 'Claude tiering timeout; defaulted to Tier 3.',
        'parse_error': 'Claude tiering parse error; defaulted to Tier 3.',
        'incomplete': 'Claude tiering incomplete response; defaulted to Tier 3.',
    }.get(kind, 'Claude tiering unavailable; defaulted to Tier 3.')


def _failure_kind(error: Exception) -> str:
    message = str(error).lower()
    if 'timeout' in message or 'timed out' in message:
        return 'timeout'
    if isinstance(error, (json.JSONDecodeError, ValueError)):
        return 'parse_error'
    return 'api_error'


def _classify_batch(leads: list[dict]) -> tuple[list[dict], Counter]:
    metrics = Counter()
    decisions_by_id = {}
    unresolved = []

    for lead in leads:
        rule_result = _deterministic_rule(lead)
        if rule_result:
            decisions_by_id[lead.get('id')] = rule_result
            metrics[f"{rule_result['decision_source']}_classified"] += 1
        else:
            unresolved.append(lead)

    if not unresolved:
        return list(decisions_by_id.values()), metrics

    pending = list(unresolved)
    for _ in range(CLAUDE_RETRIES + 1):
        try:
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{'role': 'user', 'content': _build_prompt(pending)}],
            )
            parsed = _parse_response(response.content[0].text)
            normalized = _normalize_items(parsed, pending)
            for item in normalized:
                decisions_by_id[item['lead_id']] = item
            metrics['claude_classified'] += len(normalized)
            pending = [lead for lead in pending if lead.get('id') not in decisions_by_id]
            if not pending:
                break
            metrics['fallback_incomplete'] += len(pending)
        except Exception as error:
            metrics[f"fallback_{_failure_kind(error)}"] += len(pending)
            continue

    if pending:
        kind = 'incomplete'
        if not metrics['fallback_incomplete']:
            if metrics['fallback_parse_error']:
                kind = 'parse_error'
            elif metrics['fallback_timeout']:
                kind = 'timeout'
            elif metrics['fallback_api_error']:
                kind = 'api_error'
        for item in _fallback_batch(pending, _fallback_reason(kind)):
            item['decision_source'] = 'fallback'
            decisions_by_id[item['lead_id']] = item
            metrics['fallback_classified'] += 1

    return [decisions_by_id[lead.get('id')] for lead in leads if lead.get('id') in decisions_by_id], metrics


def tier_leads(lead_ids: list[int], emit=None, wait_if_paused=None) -> dict:
    """
    Classify leads before enrichment, persist tiers, and delete hard removes.
    Returns kept/removed IDs and counts.
    """
    if emit is None:
        emit = lambda e: None

    def checkpoint():
        if wait_if_paused is not None:
            wait_if_paused()

    leads = get_leads_by_ids(lead_ids)
    lead_map = {lead.get('id'): lead for lead in leads}

    emit({'type': 'tier_start', 'count': len(leads)})

    kept_ids = []
    removed_ids = []
    tier_rows = []
    metrics = Counter()

    for start in range(0, len(leads), TIER_BATCH_SIZE):
        checkpoint()
        batch = leads[start:start + TIER_BATCH_SIZE]
        batch_decisions, batch_metrics = _classify_batch(batch)
        metrics.update(batch_metrics)

        for item in batch_decisions:
            checkpoint()
            lead = lead_map.get(item['lead_id']) or {}
            decision = item['decision']
            reason = item['reason']
            emit({
                'type': 'tier_lead',
                'lead_id': item['lead_id'],
                'company': lead.get('company', ''),
                'tier': decision,
                'tier_reason': reason,
                'decision_source': item.get('decision_source'),
            })
            if decision == TIER_HARD_REMOVE:
                removed_ids.append(item['lead_id'])
                emit({
                    'type': 'lead_removed',
                    'lead_id': item['lead_id'],
                    'company': lead.get('company', ''),
                    'tier_reason': reason,
                })
                metrics['hard_removed'] += 1
            else:
                kept_ids.append(item['lead_id'])
                tier_rows.append((item['lead_id'], decision, reason))

    conn = get_connection()
    try:
        ensure_tiering_columns(conn)
        update_lead_tiers(conn, tier_rows)
        delete_leads(conn, removed_ids)
    finally:
        conn.close()

    emit({
        'type': 'tier_done',
        'kept': len(kept_ids),
        'removed': len(removed_ids),
        'metrics': dict(metrics),
    })

    return {
        'kept_ids': kept_ids,
        'removed_ids': removed_ids,
        'kept': len(kept_ids),
        'removed': len(removed_ids),
        'metrics': dict(metrics),
    }
