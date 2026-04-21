"""Regression tests for CSV import safeguards."""
from unittest.mock import MagicMock, patch


def test_import_csv_keeps_raw_row_and_drops_company_linkedin_from_owner_field():
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
