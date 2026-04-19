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
