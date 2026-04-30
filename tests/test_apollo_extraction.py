"""Verify Apollo extracts the full set of person + organization fields."""
from unittest.mock import patch, MagicMock


APOLLO_RESPONSE = {
    'person': {
        'name': 'Jane Smith',
        'email': 'jane@acmecarwash.com',
        'phone_numbers': [{'sanitized_number': '+13105551234'}],
        'linkedin_url': 'https://linkedin.com/in/jane-smith',
        'organization': {
            'founded_year': 1998,
            'annual_revenue_printed': '13.2M',
            'short_description': 'Family-owned car wash in Long Beach.',
            'estimated_num_employees': 24,
            'facebook_url': 'https://facebook.com/acmecarwash',
            'keywords': ['car wash', 'auto detailing', 'hand wash', 'express',
                         'full service', 'waxing', 'interior cleaning', 'polishing',
                         'tire shine', 'ceramic coating', 'eleventh_keyword'],
            'primary_phone': {'sanitized_number': '+13105550000'},
        },
    },
    'people': [
        {'name': 'Jane Smith'},
        {'name': 'Bob Jones'},
    ],
}


def _run_apollo_with_response(lead, response_json):
    import pipeline.enrichment as mod
    mod.reset_x402_flag()
    with patch.object(mod, '_x402_session') as mock_session_fn:
        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = response_json
        session.post.return_value = resp
        mock_session_fn.return_value = session
        enriched = {}
        meta = {}
        cost = mod._step_apollo(lead, enriched, meta)
        return enriched, meta, cost, session


def test_apollo_extracts_all_person_fields():
    lead = {'company': 'Acme Car Wash', 'website': 'https://acmecarwash.com'}
    enriched, meta, cost, _ = _run_apollo_with_response(lead, APOLLO_RESPONSE)
    assert enriched['owner_name'] == 'Jane Smith'
    assert enriched['owner_email'] == 'jane@acmecarwash.com'
    assert enriched['owner_phone'] == '+13105551234'
    assert enriched['owner_linkedin'] == 'https://linkedin.com/in/jane-smith'
    assert cost == 0.01


def test_apollo_extracts_organization_fields():
    lead = {'company': 'Acme Car Wash', 'website': 'https://acmecarwash.com'}
    enriched, _, _, _ = _run_apollo_with_response(lead, APOLLO_RESPONSE)
    assert enriched['year_established'] == 1998
    assert enriched['revenue_estimate'] == '13.2M'
    assert enriched['company_description'] == 'Family-owned car wash in Long Beach.'
    assert enriched['employee_count'] == 24
    assert enriched['facebook_url'] == 'https://facebook.com/acmecarwash'
    assert enriched['company_phone'] == '+13105550000'


def test_apollo_services_offered_capped_at_10():
    lead = {'company': 'Acme Car Wash', 'website': 'https://acmecarwash.com'}
    enriched, _, _, _ = _run_apollo_with_response(lead, APOLLO_RESPONSE)
    assert enriched['services_offered'] == [
        'car wash', 'auto detailing', 'hand wash', 'express',
        'full service', 'waxing', 'interior cleaning', 'polishing',
        'tire shine', 'ceramic coating',
    ]
    assert len(enriched['services_offered']) == 10


def test_apollo_passes_first_last_name_when_owner_known():
    lead = {'company': 'Acme Car Wash',
            'website': 'https://acmecarwash.com',
            'owner_name': 'Jane Smith'}
    _, _, _, session = _run_apollo_with_response(lead, APOLLO_RESPONSE)
    call = session.post.call_args
    payload = call.kwargs['json']
    assert payload['first_name'] == 'Jane'
    assert payload['last_name'] == 'Smith'
    assert payload['name'] == 'Jane Smith'


def test_apollo_key_staff_from_people_list():
    lead = {'company': 'Acme Car Wash', 'website': 'https://acmecarwash.com'}
    enriched, _, _, _ = _run_apollo_with_response(lead, APOLLO_RESPONSE)
    assert enriched['key_staff'] == ['Jane Smith', 'Bob Jones']


def test_apollo_payload_omits_async_phone_flags_and_separates_company_linkedin():
    lead = {
        'company': 'Acme Car Wash',
        'website': 'https://acmecarwash.com',
        'owner_name': 'Jane Smith',
        'owner_linkedin': 'https://linkedin.com/company/acme-car-wash',
        'company_linkedin': 'https://linkedin.com/company/acme-car-wash',
    }
    _, _, _, session = _run_apollo_with_response(lead, APOLLO_RESPONSE)
    payload = session.post.call_args.kwargs['json']
    assert 'reveal_phone_number' not in payload
    assert 'run_waterfall_phone' not in payload
    assert 'run_waterfall_email' not in payload
    assert 'linkedin_url' not in payload
    assert payload['organization_linkedin_url'] == 'https://linkedin.com/company/acme-car-wash'
