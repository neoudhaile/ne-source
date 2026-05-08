"""Verify Hunter owner selection logic picks executives first, ranks by role,
and returns all people in key_staff."""
from unittest.mock import patch, MagicMock


HUNTER_RESPONSE_MIXED = {
    'data': {
        'emails': [
            {'value': 'sales@acme.com', 'first_name': 'Jake', 'last_name': 'Rep',
             'position': 'Sales Rep', 'seniority': 'junior', 'confidence': 95},
            {'value': 'jane@acme.com', 'first_name': 'Jane', 'last_name': 'Smith',
             'position': 'President', 'seniority': 'executive', 'confidence': 80,
             'phone_number': '+13105551234', 'linkedin': 'https://linkedin.com/in/jane'},
            {'value': 'mike@acme.com', 'first_name': 'Mike', 'last_name': 'Founder',
             'position': 'Owner / Founder', 'seniority': 'executive', 'confidence': 75},
            {'value': 'bob@acme.com', 'first_name': 'Bob', 'last_name': 'VP',
             'position': 'VP Operations', 'seniority': 'executive', 'confidence': 70},
        ],
    },
}

HUNTER_RESPONSE_NO_EXECS = {
    'data': {
        'emails': [
            {'value': 'sales@acme.com', 'first_name': 'Jake', 'last_name': 'Rep',
             'position': 'Sales Rep', 'seniority': 'junior', 'confidence': 90},
            {'value': 'mgr@acme.com', 'first_name': 'Sue', 'last_name': 'Boss',
             'position': 'Manager', 'seniority': 'senior', 'confidence': 60},
        ],
    },
}


def _run_hunter_with_response(lead, response_json):
    import pipeline.enrichment as mod
    mod.reset_x402_flag()
    # email-finder follow-up returns no data so we isolate domain-search
    with patch.object(mod, '_provider_get_json', side_effect=[response_json, {'data': {}}, {'data': {}}, {'data': {}}]):
        enriched = {}
        meta = {}
        cost = mod._step_hunter(lead, enriched, meta)
        return enriched, meta, cost


def test_hunter_selects_owner_over_president():
    # owner/founder beats president even with lower confidence
    lead = {'company': 'Acme', 'website': 'https://acme.com'}
    enriched, _, _ = _run_hunter_with_response(lead, HUNTER_RESPONSE_MIXED)
    assert enriched['owner_email'] == 'mike@acme.com'
    assert enriched['owner_name'] == 'Mike Founder'


def test_hunter_extracts_phone_and_linkedin_from_selection():
    # Jane is president with phone/linkedin, but Mike (owner) wins.
    # Use a response where the owner has phone/linkedin
    response = {
        'data': {
            'emails': [
                {'value': 'mike@acme.com', 'first_name': 'Mike', 'last_name': 'Founder',
                 'position': 'Owner', 'seniority': 'executive', 'confidence': 85,
                 'phone_number': '+13105559999',
                 'linkedin': 'https://linkedin.com/in/mike'},
            ],
        },
    }
    lead = {'company': 'Acme', 'website': 'https://acme.com'}
    enriched, _, _ = _run_hunter_with_response(lead, response)
    assert enriched['owner_phone'] == '+13105559999'
    assert enriched['owner_linkedin'] == 'https://linkedin.com/in/mike'


def test_hunter_falls_back_to_highest_confidence_when_no_execs():
    lead = {'company': 'Acme', 'website': 'https://acme.com'}
    enriched, _, _ = _run_hunter_with_response(lead, HUNTER_RESPONSE_NO_EXECS)
    assert enriched['owner_email'] == 'sales@acme.com'


def test_hunter_key_staff_includes_all_with_positions():
    lead = {'company': 'Acme', 'website': 'https://acme.com'}
    enriched, _, _ = _run_hunter_with_response(lead, HUNTER_RESPONSE_MIXED)
    staff = enriched['key_staff']
    assert 'Jane Smith — President' in staff
    assert 'Mike Founder — Owner / Founder' in staff
    assert 'Bob VP — VP Operations' in staff
    assert 'Jake Rep — Sales Rep' in staff
