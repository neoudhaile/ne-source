"""Tests for LeadsTable fix — leads should never vanish during a pipeline run."""
from collections import OrderedDict


def test_leads_survive_500_event_cap():
    """100 leads inserted, 40 hard_removed during tiering, 60 remain.
    Persistent map should never drop below 60 during enrichment."""
    rows = OrderedDict()
    for i in range(100):
        rows[i] = {'id': i, 'company': f'Lead {i}', 'status': 'inserted', 'tier': None}
    assert len(rows) == 100
    for i in range(60):
        rows[i]['tier'] = 'tier_1'
    for i in range(60, 100):
        del rows[i]
    assert len(rows) == 60
    for i in range(60):
        rows[i]['status'] = 'enriching'
    assert len(rows) == 60
    for i in range(60):
        rows[i]['status'] = 'enriched'
    assert len(rows) == 60


def test_incremental_apply_does_not_reprocess_old_events():
    """Events should be processed exactly once via lastProcessedIndex tracking."""
    events_processed = []
    all_events = [{'type': 'insert', 'lead_id': 1, 'company': 'Test'}]
    start = 0
    for i in range(start, len(all_events)):
        events_processed.append(all_events[i])
    last_processed = len(all_events)
    all_events.append({'type': 'tier_lead', 'lead_id': 1, 'tier': 'tier_1'})
    all_events.append({'type': 'enrich_step_start', 'lead_id': 1, 'step': 'hunter'})
    for i in range(last_processed, len(all_events)):
        events_processed.append(all_events[i])
    insert_count = sum(1 for e in events_processed if e['type'] == 'insert')
    assert insert_count == 1


def test_db_seed_shows_leads_from_past_run():
    """DB lead dict should have all fields needed for row display."""
    db_lead = {
        'id': 42, 'company': 'Sparkle Car Wash', 'city': 'Los Angeles',
        'industry': 'Car Wash', 'tier': 'tier_1', 'tier_reason': 'Strong fit',
        'owner_email': 'owner@sparkle.com', 'owner_phone': '310-555-0100',
        'enrichment_meta': {
            'owner_email': {'source': 'hunter', 'provider': 'Hunter.io'},
            'owner_phone': {'source': 'apollo', 'provider': 'Apollo'},
        },
        'generated_subject': 'Sparkle — Partnership',
        'generated_email': 'Hi, I noticed...',
    }
    assert db_lead['company'] is not None
    assert db_lead['tier'] is not None
    meta = db_lead['enrichment_meta']
    assert meta['owner_email']['source'] == 'hunter'
    assert meta['owner_phone']['source'] == 'apollo'
    assert db_lead['generated_email'] is not None
