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
    assert mod.OWNER_FIELD_SOURCE_PRIORITY['owner_name']['scrape'] == 95


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
         patch.object(mod, '_step_domain_recovery', side_effect=record('domain_recovery')), \
         patch.object(mod, '_step_scrape_website', side_effect=record('scrape_website')), \
         patch.object(mod, '_step_openmart_company', side_effect=record('openmart_company')), \
         patch.object(mod, '_step_apollo', side_effect=record('apollo')), \
         patch.object(mod, '_step_hunter', side_effect=record('hunter')), \
         patch.object(mod, '_step_sixtyfour', side_effect=record('sixtyfour')), \
         patch.object(mod, '_step_owner_email_followup', side_effect=record('owner_email_followup')), \
         patch.object(mod, '_step_fullenrich', side_effect=record('fullenrich')), \
         patch.object(mod, '_step_scrape_reviews', side_effect=record('scrape_reviews')), \
         patch.object(mod, '_step_company_contact_fallback', side_effect=record('company_fallback')), \
         patch.object(mod, '_step_claude_failsafe', side_effect=record('claude_failsafe')):
        mod.enrich_lead(1)

    assert call_order.index('google_places') < call_order.index('scrape_website')
    assert call_order.index('google_maps') < call_order.index('domain_recovery')
    assert call_order.index('domain_recovery') < call_order.index('scrape_website')
    assert call_order.index('scrape_website') < call_order.index('openmart_company')
    assert call_order.index('scrape_website') < call_order.index('apollo')
    assert call_order.index('scrape_website') < call_order.index('hunter')
    assert call_order.index('openmart_company') < call_order.index('sixtyfour')
    assert call_order.index('apollo') < call_order.index('sixtyfour')
    assert call_order.index('hunter') < call_order.index('sixtyfour')
    assert call_order.index('sixtyfour') < call_order.index('fullenrich')
    assert call_order.index('sixtyfour') < call_order.index('owner_email_followup')
    assert call_order.index('owner_email_followup') < call_order.index('fullenrich')
    assert call_order.index('fullenrich') < call_order.index('scrape_reviews')
    assert call_order.index('scrape_reviews') < call_order.index('company_fallback')
    assert call_order.index('company_fallback') < call_order.index('claude_failsafe')


def test_owner_contact_merge_prefers_hunter_identity_and_apollo_phone():
    """Hunter wins identity fields, Apollo wins phone, FullEnrich fills later gaps."""
    import pipeline.enrichment as mod

    def apollo_side_effect(lead, enriched, meta):
        enriched['owner_name'] = 'Apollo Jane'
        meta['owner_name'] = {'source': 'apollo'}
        enriched['owner_phone'] = '555-APOLLO'
        meta['owner_phone'] = {'source': 'apollo'}
        return 0.01

    def openmart_side_effect(lead, enriched, meta):
        enriched['owner_name'] = 'Openmart Jane'
        meta['owner_name'] = {'source': 'openmart'}
        enriched['owner_phone'] = '555-OPENMART'
        meta['owner_phone'] = {'source': 'openmart'}
        return 0.01

    def hunter_side_effect(lead, enriched, meta):
        enriched['owner_name'] = 'Hunter Jane'
        meta['owner_name'] = {'source': 'hunter'}
        enriched['owner_email'] = 'hunter@example.com'
        meta['owner_email'] = {'source': 'hunter'}
        return 0.01

    def fullenrich_side_effect(lead, enriched, meta):
        mod._merge(
            enriched,
            meta,
            {
                'owner_name': 'FE Jane',
                'owner_linkedin': 'https://linkedin.com/fe',
            },
            'fullenrich',
        )
        return 0.10

    lead = {'id': 1, 'company': 'Acme', 'website': 'https://acme.com'}
    captured = {}

    def fake_update(lead_id, fields):
        captured.update(fields)

    with patch.object(mod, 'get_lead', return_value=lead), \
         patch.object(mod, 'update_lead', side_effect=fake_update), \
         patch.object(mod, '_step_google_places', return_value=0.0), \
         patch.object(mod, '_step_google_maps', return_value=0.0), \
         patch.object(mod, '_step_domain_recovery', return_value=0.0), \
         patch.object(mod, '_step_scrape_website', return_value=0.0), \
         patch.object(mod, '_step_openmart_company', side_effect=openmart_side_effect), \
         patch.object(mod, '_step_apollo', side_effect=apollo_side_effect), \
         patch.object(mod, '_step_hunter', side_effect=hunter_side_effect), \
         patch.object(mod, '_step_sixtyfour', return_value=0.0), \
         patch.object(mod, '_step_owner_email_followup', return_value=0.0), \
         patch.object(mod, '_step_fullenrich', side_effect=fullenrich_side_effect), \
         patch.object(mod, '_step_scrape_reviews', return_value=0.0), \
         patch.object(mod, '_step_company_contact_fallback', return_value=0.0), \
         patch.object(mod, '_step_claude_failsafe', return_value=0.0):
        mod.enrich_lead(1)

    assert captured['owner_name'] == 'Hunter Jane'
    assert captured['owner_email'] == 'hunter@example.com'
    assert captured['owner_phone'] == '555-APOLLO'
    assert captured['owner_linkedin'] == 'https://linkedin.com/fe'


def test_openmart_company_enrich_extracts_owner_contacts():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {}
    payload = {
        'decision_makers': [
            {'name': 'Staff Person', 'title': 'Manager', 'email': 'staff@acme.com'},
            {
                'first_name': 'Jane',
                'last_name': 'Smith',
                'title': 'Owner',
                'verified_email': 'jane@acme.com',
                'direct_phone': '+13105551234',
                'linkedin_url': 'https://linkedin.com/in/janesmith',
            },
        ]
    }

    with patch('pipeline.openmart.enrich_company', return_value=payload):
        cost = mod._step_openmart_company(
            {'company': 'Acme', 'website': 'https://acme.com'},
            enriched,
            meta,
        )

    assert cost == 0.01
    assert enriched == {
        'owner_name': 'Jane Smith',
        'owner_email': 'jane@acme.com',
        'owner_phone': '+13105551234',
        'owner_linkedin': 'https://linkedin.com/in/janesmith',
    }
    assert meta['owner_name']['source'] == 'openmart'


def test_company_contact_fallback_only_backfills_owner_email():
    import pipeline.enrichment as mod

    lead = {
        'company_email': 'info@acme.com',
        'company_phone': '555-COMPANY',
    }
    enriched = {}
    meta = {}

    mod._step_company_contact_fallback(lead, enriched, meta)

    assert enriched['owner_email'] == 'info@acme.com'
    assert meta['owner_email']['source'] == 'company_fallback'
    assert 'owner_phone' not in enriched


def test_sixtyfour_tries_company_enrich_without_owner_name():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {}
    with patch.object(mod, '_provider_post_json', return_value={}):
        cost = mod._step_sixtyfour(
            {'company': 'Acme', 'website': 'https://acme.com'},
            enriched,
            meta,
        )

    assert cost == 0.10
    assert enriched == {}
    assert meta['__skip_reason'] == 'Phone provider skipped — no owner or senior decision-maker name found'


def test_sixtyfour_skips_without_verified_domain():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {}
    cost = mod._step_sixtyfour(
        {'company': 'Acme', 'owner_name': 'Jane Smith'},
        enriched,
        meta,
    )

    assert cost == 0.0
    assert enriched == {}
    assert meta['__skip_reason'] == 'Phone provider skipped — no verified domain'


def test_sixtyfour_find_phone_only_sets_owner_phone():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {}
    with patch.object(mod, '_provider_post_json', return_value={'phone': '+13105551234'}) as provider_post:
        cost = mod._step_sixtyfour(
            {
                'company': 'Acme',
                'owner_name': 'Jane Smith',
                'owner_email': 'jane@acme.com',
                'owner_linkedin': 'https://linkedin.com/in/janesmith',
                'website': 'https://acme.com',
                'city': 'LA',
                'state': 'CA',
            },
            enriched,
            meta,
        )

    assert cost == 0.30
    assert enriched == {'owner_phone': '+13105551234'}
    assert meta['owner_phone']['source'] == 'sixtyfour'
    assert provider_post.call_count == 1
    assert provider_post.call_args.args[:2] == ('sixtyfour', '/find-phone')
    payload = provider_post.call_args.kwargs['body']
    assert payload['email'] == 'jane@acme.com'
    assert payload['linkedin_url'] == 'https://linkedin.com/in/janesmith'


def test_owner_email_followup_fills_hunter_email_for_grounded_person_domain():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {'owner_name': {'source': 'scrape'}}
    with patch.object(mod, '_provider_get_json', return_value={
        'data': {
            'email': 'jane@acme.com',
            'first_name': 'Jane',
            'last_name': 'Smith',
        }
    }) as provider_get, \
         patch.object(mod, '_provider_post_json') as provider_post:
        cost = mod._step_owner_email_followup(
            {'company': 'Acme', 'owner_name': 'Jane Smith', 'website': 'https://acme.com'},
            enriched,
            meta,
        )

    assert cost == 0.01
    assert enriched['owner_email'] == 'jane@acme.com'
    assert meta['owner_email']['source'] == 'hunter'
    provider_get.assert_called_once()
    provider_post.assert_not_called()


def test_owner_email_followup_tries_sixtyfour_when_hunter_empty():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {'owner_name': {'source': 'scrape'}}
    with patch.object(mod, '_provider_get_json', return_value={'data': {}}), \
         patch.object(mod, '_provider_post_json', return_value={'person': {'name': 'Jane Smith', 'email': 'jane@acme.com'}}) as provider_post:
        cost = mod._step_owner_email_followup(
            {'company': 'Acme', 'owner_name': 'Jane Smith', 'website': 'https://acme.com'},
            enriched,
            meta,
        )

    assert cost == 0.31
    assert enriched['owner_email'] == 'jane@acme.com'
    assert meta['owner_email']['source'] == 'sixtyfour'
    assert provider_post.call_args.args[:2] == ('sixtyfour', '/find-email')


def test_owner_email_followup_treats_company_fallback_as_missing():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {'owner_name': {'source': 'scrape'}, 'owner_email': {'source': 'company_fallback'}}
    lead = {
        'company': 'Acme',
        'owner_name': 'Jane Smith',
        'owner_email': 'info@acme.com',
        'website': 'https://acme.com',
    }
    with patch.object(mod, '_provider_get_json', return_value={'data': {'email': 'jane@acme.com'}}), \
         patch.object(mod, '_provider_post_json'):
        mod._step_owner_email_followup(lead, enriched, meta)

    assert enriched['owner_email'] == 'jane@acme.com'
    assert meta['owner_email']['source'] == 'hunter'
