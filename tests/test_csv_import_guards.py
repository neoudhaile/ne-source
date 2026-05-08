"""Regression tests for CSV import safeguards."""
from unittest.mock import MagicMock, patch


def test_import_csv_keeps_company_linkedin_out_of_owner_field_and_preserves_it():
    import pipeline.csv_import as mod

    csv_text = (
        'Name,LinkedIn,Address\n'
        '"Acme Utilities","https://www.linkedin.com/company/acme-utilities","123 Main St, Los Angeles, CA 90001"\n'
    )

    captured = {}

    def fake_insert_batch(conn, lead_dicts):
        captured['lead_dicts'] = lead_dicts
        return {lead['google_place_id']: index + 1 for index, lead in enumerate(lead_dicts)}

    with patch.object(mod, 'get_connection', return_value=MagicMock()), \
         patch.object(mod, 'insert_leads_csv_batch', side_effect=fake_insert_batch):
        result = mod.import_csv(csv_text)

    assert result['inserted'] == 1
    lead = captured['lead_dicts'][0]
    assert 'owner_linkedin' not in lead or lead['owner_linkedin'] is None
    assert lead['company_linkedin'] == 'https://www.linkedin.com/company/acme-utilities'
    assert 'linkedin.com/company/acme-utilities' in lead['raw_data']


def test_import_csv_maps_person_linkedin_profile_to_owner_linkedin():
    import pipeline.csv_import as mod

    csv_text = (
        'Name,LinkedIn\n'
        'Acme Utilities,https://www.linkedin.com/in/jane-smith\n'
    )

    captured = {}

    def fake_insert_batch(conn, lead_dicts):
        captured['lead_dicts'] = lead_dicts
        return {lead['google_place_id']: index + 1 for index, lead in enumerate(lead_dicts)}

    with patch.object(mod, 'get_connection', return_value=MagicMock()), \
         patch.object(mod, 'insert_leads_csv_batch', side_effect=fake_insert_batch):
        mod.import_csv(csv_text)

    lead = captured['lead_dicts'][0]
    assert lead['owner_linkedin'] == 'https://www.linkedin.com/in/jane-smith'


def test_import_csv_keeps_employee_bucket_ranges_out_of_employee_count():
    import pipeline.csv_import as mod

    csv_text = (
        'Name,Employees\n'
        'Acme Utilities,51 - 200\n'
    )

    captured = {}

    def fake_insert_batch(conn, lead_dicts):
        captured['lead_dicts'] = lead_dicts
        return {lead['google_place_id']: index + 1 for index, lead in enumerate(lead_dicts)}

    with patch.object(mod, 'get_connection', return_value=MagicMock()), \
         patch.object(mod, 'insert_leads_csv_batch', side_effect=fake_insert_batch):
        mod.import_csv(csv_text)

    lead = captured['lead_dicts'][0]
    assert 'employee_count' not in lead or lead['employee_count'] is None
    assert '"Employees": "51 - 200"' in lead['raw_data']


def test_import_csv_parses_full_address_with_country_before_zip():
    import pipeline.csv_import as mod

    csv_text = (
        'Name,Address\n'
        '"Coast to Coast Business Equipment","8 Vanderbilt, Irvine, California, United States, 92618"\n'
    )

    captured = {}

    def fake_insert_batch(conn, lead_dicts):
        captured['lead_dicts'] = lead_dicts
        return {lead['google_place_id']: index + 1 for index, lead in enumerate(lead_dicts)}

    with patch.object(mod, 'get_connection', return_value=MagicMock()), \
         patch.object(mod, 'insert_leads_csv_batch', side_effect=fake_insert_batch):
        mod.import_csv(csv_text)

    lead = captured['lead_dicts'][0]
    assert lead['address'] == '8 Vanderbilt'
    assert lead['city'] == 'Irvine'
    assert lead['state'] == 'California'
    assert lead['zipcode'] == '92618'
