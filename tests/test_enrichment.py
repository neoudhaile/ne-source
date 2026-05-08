"""Tests for enrichment pipeline optimizations."""
import threading
import time
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.fixture(autouse=True)
def _disable_unrelated_late_paid_steps(monkeypatch):
    import pipeline.enrichment as mod

    monkeypatch.setattr(mod, '_step_sixtyfour', lambda l, e, m: 0.0)
    monkeypatch.setattr(mod, '_step_fullenrich', lambda l, e, m: 0.0)


def test_x402_session_reuse_same_thread():
    """Same thread should get the same session object back."""
    import pipeline.enrichment as mod
    if hasattr(mod, '_thread_local'):
        if hasattr(mod._thread_local, 'x402_session'):
            del mod._thread_local.x402_session

    with patch('pipeline.enrichment.Account') as mock_account, \
         patch('pipeline.enrichment.x402ClientSync') as mock_client_cls, \
         patch('pipeline.enrichment.x402_http_adapter') as mock_adapter:

        mock_account.from_key.return_value = MagicMock()
        mock_client_cls.return_value = MagicMock()
        mock_adapter.return_value = MagicMock()

        session_a = mod._x402_session()
        session_b = mod._x402_session()
        assert session_a is session_b, "Same thread should reuse the session"
        assert mock_account.from_key.call_count == 1


def test_x402_session_different_threads():
    """Different threads should get different session objects."""
    import pipeline.enrichment as mod

    sessions = {}
    errors = []

    def grab(name):
        try:
            with patch('pipeline.enrichment.Account') as mock_account, \
                 patch('pipeline.enrichment.x402ClientSync') as mock_client_cls, \
                 patch('pipeline.enrichment.x402_http_adapter') as mock_adapter:
                mock_account.from_key.return_value = MagicMock()
                mock_client_cls.return_value = MagicMock()
                mock_adapter.return_value = MagicMock()
                sessions[name] = mod._x402_session()
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=grab, args=('t1',))
    t2 = threading.Thread(target=grab, args=('t2',))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert not errors, f"Thread errors: {errors}"
    assert sessions['t1'] is not sessions['t2'], "Different threads must have different sessions"


def test_x402_session_not_context_manager():
    """_x402_session should return a plain Session, not require 'with' statement."""
    import pipeline.enrichment as mod
    if hasattr(mod, '_thread_local'):
        if hasattr(mod._thread_local, 'x402_session'):
            del mod._thread_local.x402_session

    with patch('pipeline.enrichment.Account') as mock_account, \
         patch('pipeline.enrichment.x402ClientSync') as mock_client_cls, \
         patch('pipeline.enrichment.x402_http_adapter') as mock_adapter:

        mock_account.from_key.return_value = MagicMock()
        mock_client_cls.return_value = MagicMock()
        mock_adapter.return_value = MagicMock()

        session = mod._x402_session()
        assert hasattr(session, 'get'), "Session should have .get method"
        assert hasattr(session, 'post'), "Session should have .post method"


def test_step_registry_allows_patching():
    """Patching a step function should affect enrich_lead behavior."""
    import pipeline.enrichment as mod

    call_log = []

    def fake_hunter(lead, enriched, meta):
        call_log.append('hunter_called')
        enriched['owner_email'] = 'patched@test.com'
        meta['owner_email'] = {'source': 'hunter'}
        return 0.01

    with patch.object(mod, '_step_hunter', fake_hunter), \
         patch.object(mod, '_step_google_places', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_google_maps', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_domain_recovery', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_openmart_company', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_apollo', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_website', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', return_value={'id': 1, 'company': 'Test Corp'}), \
         patch.object(mod, 'update_lead'):

        result = mod.enrich_lead(1)

    assert 'hunter_called' in call_log, "Patched hunter should have been called"
    assert result['sources'].get('owner_email', {}).get('source') == 'hunter'


def test_all_steps_execute_in_order():
    """All enrichment stages should execute in the expected overall order."""
    import pipeline.enrichment as mod

    executed_steps = []

    def make_step(name):
        def step_fn(lead, enriched, meta):
            executed_steps.append(name)
            return 0.0
        return step_fn

    with patch.object(mod, '_step_google_places', make_step('google_places')), \
         patch.object(mod, '_step_google_maps', make_step('google_maps')), \
         patch.object(mod, '_step_domain_recovery', make_step('domain_recovery')), \
         patch.object(mod, '_step_openmart_company', make_step('openmart_company')), \
         patch.object(mod, '_step_hunter', make_step('hunter')), \
         patch.object(mod, '_step_apollo', make_step('apollo')), \
         patch.object(mod, '_step_scrape_website', make_step('scrape_website')), \
         patch.object(mod, '_step_owner_email_followup', make_step('owner_email_followup')), \
         patch.object(mod, '_step_scrape_reviews', make_step('scrape_reviews')), \
         patch.object(mod, '_step_company_contact_fallback', make_step('company_fallback')), \
         patch.object(mod, '_step_claude_failsafe', make_step('claude_failsafe')), \
         patch.object(mod, 'get_lead', return_value={'id': 1, 'company': 'Test'}), \
         patch.object(mod, 'update_lead'):

        mod.enrich_lead(1)

    assert executed_steps.index('google_places') < executed_steps.index('domain_recovery')
    assert executed_steps.index('google_maps') < executed_steps.index('domain_recovery')
    assert executed_steps.index('domain_recovery') < executed_steps.index('scrape_website')
    assert executed_steps.index('scrape_website') < executed_steps.index('openmart_company')
    assert executed_steps.index('scrape_website') < executed_steps.index('apollo')
    assert executed_steps.index('scrape_website') < executed_steps.index('hunter')
    assert executed_steps.index('openmart_company') < executed_steps.index('scrape_reviews')
    assert executed_steps.index('apollo') < executed_steps.index('scrape_reviews')
    assert executed_steps.index('hunter') < executed_steps.index('scrape_reviews')
    assert executed_steps.index('owner_email_followup') < executed_steps.index('scrape_reviews')
    assert executed_steps.index('scrape_reviews') < executed_steps.index('company_fallback')
    assert executed_steps.index('company_fallback') < executed_steps.index('claude_failsafe')


def test_phase2_runs_parallel():
    """Owner-contact steps should overlap in time."""
    import pipeline.enrichment as mod

    call_times = {}

    def make_slow_step(name):
        def step_fn(lead, enriched, meta):
            call_times[name] = {'start': time.monotonic()}
            time.sleep(0.3)
            call_times[name]['end'] = time.monotonic()
            enriched[f'{name}_field'] = f'{name}_value'
            meta[f'{name}_field'] = {'source': name}
            return 0.01
        return step_fn

    with patch.object(mod, '_step_google_places', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_google_maps', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_domain_recovery', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_openmart_company', make_slow_step('openmart_company')), \
         patch.object(mod, '_step_hunter', make_slow_step('hunter')), \
         patch.object(mod, '_step_apollo', make_slow_step('apollo')), \
         patch.object(mod, '_step_scrape_website', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', return_value={'id': 1, 'company': 'Test Corp'}), \
         patch.object(mod, 'update_lead'):

        result = mod.enrich_lead(1)

    assert set(call_times.keys()) == {'openmart_company', 'hunter', 'apollo'}

    starts = [call_times[k]['start'] for k in ['openmart_company', 'hunter', 'apollo']]
    max_gap = max(starts) - min(starts)
    assert max_gap < 0.15, f"Phase 2 steps should start near-simultaneously, gap was {max_gap:.3f}s"

    earliest_start = min(starts)
    latest_end = max(call_times[k]['end'] for k in ['openmart_company', 'hunter', 'apollo'])
    wall_time = latest_end - earliest_start
    assert wall_time < 0.6, f"Phase 2 wall time should be ~0.3s parallel, was {wall_time:.3f}s"

    assert result['cost'] > 0


def test_phase2_merge_order_deterministic():
    """When multiple phase-2 steps set the same field, hunter wins (first in merge order)."""
    import pipeline.enrichment as mod

    def hunter_email(lead, enriched, meta):
        enriched['owner_email'] = 'hunter@test.com'
        meta['owner_email'] = {'source': 'hunter'}
        return 0.01

    def apollo_email(lead, enriched, meta):
        enriched['owner_email'] = 'apollo@test.com'
        meta['owner_email'] = {'source': 'apollo'}
        return 0.01

    with patch.object(mod, '_step_google_places', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_google_maps', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_domain_recovery', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_openmart_company', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_hunter', hunter_email), \
         patch.object(mod, '_step_apollo', apollo_email), \
         patch.object(mod, '_step_scrape_website', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', return_value={'id': 1, 'company': 'Test'}), \
         patch.object(mod, 'update_lead'):

        result = mod.enrich_lead(1)

    assert result['sources']['owner_email']['source'] == 'hunter'


def test_phase2_error_isolation():
    """An error in one phase-2 step should not abort the others."""
    import pipeline.enrichment as mod

    def failing_hunter(lead, enriched, meta):
        raise RuntimeError("Hunter API down")

    def working_apollo(lead, enriched, meta):
        enriched['owner_linkedin'] = 'https://linkedin.com/test'
        meta['owner_linkedin'] = {'source': 'apollo'}
        return 0.01

    with patch.object(mod, '_step_google_places', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_google_maps', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_domain_recovery', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_openmart_company', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_hunter', failing_hunter), \
         patch.object(mod, '_step_apollo', working_apollo), \
         patch.object(mod, '_step_scrape_website', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', return_value={'id': 1, 'company': 'Test'}), \
         patch.object(mod, 'update_lead'):

        result = mod.enrich_lead(1)

    assert result['sources'].get('owner_linkedin', {}).get('source') == 'apollo'


def test_phase1_runs_before_phase2():
    """Phase 1 must complete before Phase 2 starts."""
    import pipeline.enrichment as mod

    execution_log = []

    def phase1_step(name):
        def step_fn(lead, enriched, meta):
            execution_log.append(('p1', name, time.monotonic()))
            time.sleep(0.05)
            if name == 'google_places':
                enriched['website'] = 'https://example.com'
                meta['website'] = {'source': 'google_places'}
            return 0.0
        return step_fn

    def phase2_step(name):
        def step_fn(lead, enriched, meta):
            execution_log.append(('p2', name, time.monotonic()))
            return 0.0
        return step_fn

    with patch.object(mod, '_step_google_places', phase1_step('google_places')), \
         patch.object(mod, '_step_google_maps', phase1_step('google_maps')), \
         patch.object(mod, '_step_domain_recovery', phase1_step('domain_recovery')), \
         patch.object(mod, '_step_openmart_company', phase2_step('openmart_company')), \
         patch.object(mod, '_step_hunter', phase2_step('hunter')), \
         patch.object(mod, '_step_apollo', phase2_step('apollo')), \
         patch.object(mod, '_step_scrape_website', phase2_step('scrape_website')), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', return_value={'id': 1, 'company': 'Test'}), \
         patch.object(mod, 'update_lead'):

        mod.enrich_lead(1)

    p1_times = [t for phase, name, t in execution_log if phase == 'p1']
    p2_times = [t for phase, name, t in execution_log if phase == 'p2']
    assert max(p1_times) < min(p2_times), "Phase 1 must complete before Phase 2 starts"


def test_batch_enrichment_faster_than_sequential():
    """Batch of 3 leads with parallel phase2 should complete faster than 3x sequential."""
    import pipeline.enrichment as mod

    def make_slow_step(name):
        def step_fn(lead, enriched, meta):
            time.sleep(0.1)  # simulate API latency
            enriched[f'{name}_data'] = f'{name}_value_{lead.get("id")}'
            meta[f'{name}_data'] = {'source': name}
            return 0.01
        return step_fn

    with patch.object(mod, '_step_google_places', make_slow_step('places')), \
         patch.object(mod, '_step_google_maps', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_domain_recovery', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_openmart_company', make_slow_step('openmart')), \
         patch.object(mod, '_step_hunter', make_slow_step('hunter')), \
         patch.object(mod, '_step_apollo', make_slow_step('apollo')), \
         patch.object(mod, '_step_scrape_website', make_slow_step('scrape')), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', side_effect=lambda lid: {'id': lid, 'company': f'Co {lid}'}), \
         patch.object(mod, 'update_lead'):

        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(mod.enrich_lead, i) for i in range(1, 4)]
            results = [f.result() for f in futures]
        wall = time.monotonic() - start

        # Sequential per lead: 2 slow phase1 + 3 slow phase2 (parallel=0.1) + 0 phase3 = 0.3s
        # 3 leads sequential: 0.9s. With 3 leads in parallel: ~0.3s
        # Be generous: under 0.8s means parallelism is working
        assert wall < 0.8, f"Batch should complete in under 0.8s, took {wall:.2f}s"
        assert all(r['cost'] > 0 for r in results), "All leads should have non-zero cost"
        assert len(results) == 3


def test_batch_enrichment_data_integrity():
    """Each lead in a batch should get its own data, no cross-contamination."""
    import pipeline.enrichment as mod

    def lead_specific_step(lead, enriched, meta):
        lead_id = lead.get('id', 0)
        enriched['company_description'] = f'Description for lead {lead_id}'
        meta['company_description'] = {'source': 'test'}
        return 0.0

    updated_leads = {}

    def capture_update(lead_id, data):
        updated_leads[lead_id] = data.copy()

    with patch.object(mod, '_step_google_places', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_google_maps', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_domain_recovery', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_openmart_company', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_hunter', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_apollo', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_website', lead_specific_step), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', side_effect=lambda lid: {'id': lid, 'company': f'Co {lid}'}), \
         patch.object(mod, 'update_lead', side_effect=capture_update):

        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(mod.enrich_lead, i) for i in range(1, 4)]
            [f.result() for f in futures]

    # Each lead should have its own unique description
    for lead_id in [1, 2, 3]:
        assert updated_leads[lead_id]['company_description'] == f'Description for lead {lead_id}', \
            f"Lead {lead_id} has wrong description: {updated_leads[lead_id]['company_description']}"


def test_emit_events_fire_for_all_steps():
    """Every step should emit start + done/skip/error events."""
    import pipeline.enrichment as mod

    events = []

    def capture_emit(event):
        events.append(event)

    with patch.object(mod, '_step_google_places', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_google_maps', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_domain_recovery', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_openmart_company', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_hunter', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_apollo', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_website', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_owner_email_followup', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_scrape_reviews', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_company_contact_fallback', lambda l, e, m: 0.0), \
         patch.object(mod, '_step_claude_failsafe', lambda l, e, m: 0.0), \
         patch.object(mod, 'get_lead', return_value={'id': 1, 'company': 'Test'}), \
         patch.object(mod, 'update_lead'):

        mod.enrich_lead(1, emit=capture_emit)

    # 13 steps x 2 events each (start + done/skip) = 26 events
    start_events = [e for e in events if e['type'] == 'enrich_step_start']
    end_events = [e for e in events if e['type'] in ('enrich_step_done', 'enrich_step_skip', 'enrich_step_error')]
    assert len(start_events) == 13, f"Expected 13 start events, got {len(start_events)}"
    assert len(end_events) == 13, f"Expected 13 end events, got {len(end_events)}"
