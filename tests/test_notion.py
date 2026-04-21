"""Tests for Notion export module."""

import os
from unittest.mock import patch

import pipeline.notion as mod


def test_is_viable_lead_with_enrichment():
    lead = {'id': 1, 'company': 'Test', 'enrichment_meta': {'website': {'source': 'google'}}}
    assert mod.is_viable_lead(lead) is True


def test_is_viable_lead_without_enrichment():
    lead = {'id': 1, 'company': 'Test', 'enrichment_meta': None}
    assert mod.is_viable_lead(lead) is False


def test_is_viable_lead_empty_meta():
    lead = {'id': 1, 'company': 'Test', 'enrichment_meta': {}}
    assert mod.is_viable_lead(lead) is False


def test_lead_to_notion_properties_full():
    lead = {
        'company': 'Sparkle Wash',
        'owner_name': 'John Doe',
        'owner_email': 'john@sparkle.com',
        'owner_phone': '555-1234',
        'website': 'https://sparkle.com',
        'industry': 'Car Wash',
        'city': 'Los Angeles',
        'state': 'CA',
        'tier': 'tier_1',
        'rating': 4.5,
        'review_count': 120,
        'employee_count': 15,
        'year_established': 2005,
        'revenue_estimate': '$1M-$5M',
        'company_description': 'Full service car wash',
        'services_offered': ['wash', 'detail', 'wax'],
        'owner_linkedin': 'https://linkedin.com/in/johndoe',
        'google_maps_url': 'https://maps.google.com/...',
        'tier_reason': 'Strong fit for acquisition',
        'source': 'openmart',
    }

    props = mod._lead_to_notion_properties(lead)

    assert props['Company']['title'][0]['text']['content'] == 'Sparkle Wash'
    assert props['Owner Email']['email'] == 'john@sparkle.com'
    assert props['Owner Phone']['phone_number'] == '555-1234'
    assert props['Website']['url'] == 'https://sparkle.com'
    assert props['Industry']['select']['name'] == 'Car Wash'
    assert props['Tier']['select']['name'] == 'tier_1'
    assert props['Rating']['number'] == 4.5
    assert props['Services']['rich_text'][0]['text']['content'] == 'wash, detail, wax'


def test_lead_to_notion_properties_maps_unknown_industry_to_other():
    lead = {'company': 'Test Corp', 'industry': 'Pest Control'}

    props = mod._lead_to_notion_properties(lead)

    assert props['Industry']['select']['name'] == 'Other'


def test_lead_to_notion_properties_minimal():
    lead = {'company': 'Test Corp'}

    props = mod._lead_to_notion_properties(lead)

    assert props['Company']['title'][0]['text']['content'] == 'Test Corp'
    assert 'Owner Email' not in props


def test_export_leads_to_notion_calls_api():
    leads = [
        {'id': 1, 'company': 'Co A', 'enrichment_meta': {'x': {}}},
        {'id': 2, 'company': 'Co B', 'enrichment_meta': None},
    ]
    events = []

    with patch.object(mod, 'get_leads_by_ids', return_value=leads), \
         patch.object(mod, 'NOTION_DATABASE_ID', 'db-123'), \
         patch.dict(os.environ, {'NOTION_API_KEY': 'secret'}, clear=False), \
         patch.object(mod, '_post_to_notion', return_value={'id': 'page-1'}) as mock_post:
        result = mod.export_leads_to_notion([1, 2], emit=lambda e: events.append(e))

    assert mock_post.call_count == 1
    assert result['exported'] == 1
    assert result['skipped'] == 1
    start_events = [e for e in events if e['type'] == 'export_start']
    done_events = [e for e in events if e['type'] == 'export_done']
    assert len(start_events) == 1
    assert len(done_events) == 1


def test_export_leads_to_notion_skips_when_not_configured():
    events = []

    with patch.object(mod, 'NOTION_DATABASE_ID', ''), \
         patch.dict(os.environ, {}, clear=True):
        result = mod.export_leads_to_notion([1, 2], emit=lambda e: events.append(e))

    assert result == {'exported': 0, 'skipped': 2, 'errors': 0}
    assert events == [{'type': 'export_skip', 'reason': 'Notion export not configured'}]
