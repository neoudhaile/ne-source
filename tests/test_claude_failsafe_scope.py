"""Claude failsafe may never write owner contact fields."""
from unittest.mock import patch, MagicMock


def _run_failsafe_with_response(lead, response_text):
    import pipeline.enrichment as mod
    resp = MagicMock()
    resp.content = [MagicMock(text=response_text)]
    resp.usage = MagicMock(input_tokens=50, output_tokens=50)
    enriched = {}
    meta = {}
    with patch.object(mod.claude.messages, 'create', return_value=resp):
        mod._step_claude_failsafe(lead, enriched, meta)
    return enriched


def test_failsafe_drops_contact_fields_even_if_claude_returns_them():
    lead = {'company': 'Acme', 'industry': 'car wash', 'website': 'https://acme.com'}
    response_text = (
        '{"owner_name": "Fake Owner", "owner_email": "fake@acme.com", '
        '"owner_phone": "555-1234", "owner_linkedin": "https://linkedin.com/fake", '
        '"company_description": "Real description", "services_offered": ["wash"]}'
    )
    enriched = _run_failsafe_with_response(lead, response_text)
    assert 'owner_name' not in enriched
    assert 'owner_email' not in enriched
    assert 'owner_phone' not in enriched
    assert 'owner_linkedin' not in enriched
    assert enriched['company_description'] == 'Real description'
    assert enriched['services_offered'] == ['wash']


def test_failsafe_skips_when_row_has_no_grounded_evidence():
    import pipeline.enrichment as mod

    lead = {
        'company': 'Acme',
        'city': 'Los Angeles',
        'state': 'CA',
        'google_place_id': 'CSV_test123',
    }
    with patch.object(mod.claude.messages, 'create') as mock_create:
        enriched = {}
        meta = {}
        cost = mod._step_claude_failsafe(lead, enriched, meta)
    assert cost == 0.0
    assert enriched == {}
    mock_create.assert_not_called()
