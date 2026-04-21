"""Tests for the new 4-stage enrichment waterfall.

Each test mocks the external dependencies of the step under test."""
from unittest.mock import patch, MagicMock


def test_scrape_website_extracts_owner_name():
    import pipeline.enrichment as mod

    lead = {'website': 'https://acme.com', 'company': 'Acme'}
    enriched = {}
    meta = {}

    fake_claude_response = MagicMock()
    fake_claude_response.content = [MagicMock(text=(
        '{"owner_name": "Jane Smith", "services_offered": ["wash"], '
        '"company_description": "Family owned.", "year_established": 1998, '
        '"certifications": null, "facebook_url": null, "yelp_url": null, '
        '"employee_count": null, "company_email": null, "company_phone": null}'
    ))]
    fake_claude_response.usage = MagicMock(input_tokens=100, output_tokens=50)

    with patch.object(mod, 'has_firecrawl_api_key', return_value=True), \
         patch.object(mod, 'scrape_site_pages', return_value=[
             {'url': 'https://acme.com/about', 'markdown': 'About us: owned by Jane Smith.',
              'provider_used': 'direct'}
         ]), \
         patch.object(mod.claude.messages, 'create', return_value=fake_claude_response):
        mod._step_scrape_website(lead, enriched, meta)

    assert enriched['owner_name'] == 'Jane Smith'
    assert meta['owner_name']['source'] == 'scrape'


def test_scrape_source_priority_for_owner_name_is_above_zero():
    import pipeline.enrichment as mod
    assert mod.OWNER_FIELD_SOURCE_PRIORITY['owner_name']['scrape'] == 50


def test_stages_run_in_correct_order():
    """Every step function fires at least once, in the expected overall order."""
    import pipeline.enrichment as mod

    call_order = []

    def record(name):
        def _fn(lead, enriched, meta):
            call_order.append(name)
            return 0.0
        return _fn

    lead = {
        'id': 1,
        'company': 'Acme',
        'website': 'https://acme.com',
        'city': 'LA',
        'state': 'CA',
    }

    with patch.object(mod, 'get_lead', return_value=lead), \
         patch.object(mod, 'update_lead'), \
         patch.object(mod, '_step_google_places', side_effect=record('google_places')), \
         patch.object(mod, '_step_google_maps', side_effect=record('google_maps')), \
         patch.object(mod, '_step_scrape_website', side_effect=record('scrape_website')), \
         patch.object(mod, '_step_apollo', side_effect=record('apollo')), \
         patch.object(mod, '_step_hunter', side_effect=record('hunter')), \
         patch.object(mod, '_step_fullenrich', side_effect=record('fullenrich')), \
         patch.object(mod, '_step_scrape_reviews', side_effect=record('scrape_reviews')), \
         patch.object(mod, '_step_company_contact_fallback', side_effect=record('company_fallback')), \
         patch.object(mod, '_step_claude_failsafe', side_effect=record('claude_failsafe')):
        mod.enrich_lead(1)

    assert call_order.index('google_places') < call_order.index('scrape_website')
    assert call_order.index('scrape_website') < call_order.index('apollo')
    assert call_order.index('scrape_website') < call_order.index('hunter')
    assert call_order.index('scrape_website') < call_order.index('fullenrich')
    assert call_order.index('apollo') < call_order.index('scrape_reviews')
    assert call_order.index('hunter') < call_order.index('company_fallback')
    assert call_order.index('scrape_reviews') < call_order.index('company_fallback')
    assert call_order.index('company_fallback') < call_order.index('claude_failsafe')


def test_stage_3_merge_prefers_apollo_then_hunter_then_fullenrich():
    """When multiple Stage 3 sources return the same field, apollo wins, then hunter."""
    import pipeline.enrichment as mod

    def apollo_side_effect(lead, enriched, meta):
        enriched['owner_name'] = 'Apollo Jane'
        meta['owner_name'] = {'source': 'apollo'}
        return 0.01

    def hunter_side_effect(lead, enriched, meta):
        enriched['owner_name'] = 'Hunter Jane'
        meta['owner_name'] = {'source': 'hunter'}
        enriched['owner_phone'] = '555-HUNTER'
        meta['owner_phone'] = {'source': 'hunter'}
        return 0.01

    def fullenrich_side_effect(lead, enriched, meta):
        enriched['owner_name'] = 'FE Jane'
        meta['owner_name'] = {'source': 'fullenrich'}
        enriched['owner_linkedin'] = 'https://linkedin.com/fe'
        meta['owner_linkedin'] = {'source': 'fullenrich'}
        return 0.10

    lead = {'id': 1, 'company': 'Acme', 'website': 'https://acme.com'}
    captured = {}

    def fake_update(lead_id, fields):
        captured.update(fields)

    with patch.object(mod, 'get_lead', return_value=lead), \
         patch.object(mod, 'update_lead', side_effect=fake_update), \
         patch.object(mod, '_step_google_places', return_value=0.0), \
         patch.object(mod, '_step_google_maps', return_value=0.0), \
         patch.object(mod, '_step_scrape_website', return_value=0.0), \
         patch.object(mod, '_step_apollo', side_effect=apollo_side_effect), \
         patch.object(mod, '_step_hunter', side_effect=hunter_side_effect), \
         patch.object(mod, '_step_fullenrich', side_effect=fullenrich_side_effect), \
         patch.object(mod, '_step_scrape_reviews', return_value=0.0), \
         patch.object(mod, '_step_company_contact_fallback', return_value=0.0), \
         patch.object(mod, '_step_claude_failsafe', return_value=0.0):
        mod.enrich_lead(1)

    assert captured['owner_name'] == 'Apollo Jane'
    assert captured['owner_phone'] == '555-HUNTER'
    assert captured['owner_linkedin'] == 'https://linkedin.com/fe'
