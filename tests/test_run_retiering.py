"""Pipeline should re-tier after enrichment so final DB state reflects new evidence."""
from unittest.mock import MagicMock, patch


def test_run_csv_pipeline_retieres_after_enrichment():
    import pipeline.run as mod

    lead = {
        'id': 1,
        'company': 'Acme',
        'industry': 'Utilities',
        'city': 'LA',
        'enrichment_meta': None,
        'distance_miles': None,
        'ownership_type': None,
    }

    with patch.object(mod, 'reset_x402_flag'), \
         patch.object(mod, 'get_leads_by_ids', return_value=[lead]), \
         patch.object(mod, 'tier_leads', side_effect=[{'kept_ids': [1]}, {'kept_ids': [1]}]) as mock_tier, \
         patch.object(mod, 'check_x402_balance', return_value=100.0), \
         patch.object(mod, 'enrich_lead', return_value={'cost': 0.0, 'sources': {}}), \
         patch.object(mod, 'export_leads_to_notion'), \
         patch.object(mod, 'update_run_cost'), \
         patch.object(mod, 'get_connection', return_value=MagicMock()), \
         patch.object(mod, 'count_leads', return_value=1):
        mod.run_csv_pipeline([1], emit=lambda e: None, run_id=50)

    assert mock_tier.call_count == 2
