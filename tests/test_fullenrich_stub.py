"""FullEnrich client + step behaves correctly as a stub and as an active provider."""
import os
from unittest.mock import patch, MagicMock


def test_has_api_key_false_when_env_unset():
    import pipeline.fullenrich as fe
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop('FULLENRICH_API_KEY', None)
        assert fe.has_api_key() is False


def test_has_api_key_true_when_env_set():
    import pipeline.fullenrich as fe
    with patch.dict(os.environ, {'FULLENRICH_API_KEY': 'sk-test'}):
        assert fe.has_api_key() is True


def test_step_fullenrich_noop_without_key():
    import pipeline.enrichment as mod
    lead = {'company': 'Acme', 'website': 'https://acme.com'}
    enriched = {}
    meta = {}
    with patch('pipeline.fullenrich.has_api_key', return_value=False):
        cost = mod._step_fullenrich(lead, enriched, meta)
    assert cost == 0.0
    assert enriched == {}


def test_step_fullenrich_skips_when_all_owner_fields_filled():
    import pipeline.enrichment as mod
    lead = {'company': 'Acme', 'website': 'https://acme.com',
            'owner_name': 'X', 'owner_email': 'x@acme.com', 'owner_phone': '555'}
    enriched = {}
    meta = {}
    with patch('pipeline.fullenrich.has_api_key', return_value=True), \
         patch('pipeline.fullenrich.enrich_person') as mock_call:
        cost = mod._step_fullenrich(lead, enriched, meta)
    assert cost == 0.0
    mock_call.assert_not_called()


def test_step_fullenrich_fills_missing_owner_fields():
    import pipeline.enrichment as mod
    lead = {'company': 'Acme', 'website': 'https://acme.com'}
    enriched = {}
    meta = {}
    fake_result = {
        'owner_name': 'Jane Smith',
        'owner_email': 'jane@acme.com',
        'owner_phone': '+13105551234',
        'owner_linkedin': 'https://linkedin.com/in/jane',
    }
    with patch('pipeline.fullenrich.has_api_key', return_value=True), \
         patch('pipeline.fullenrich.enrich_person', return_value=fake_result):
        cost = mod._step_fullenrich(lead, enriched, meta)
    assert cost > 0
    assert enriched['owner_name'] == 'Jane Smith'
    assert enriched['owner_email'] == 'jane@acme.com'
    assert meta['owner_email']['source'] == 'fullenrich'
