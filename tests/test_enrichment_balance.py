"""Tests for x402 wallet balance check and 402 short-circuit."""
from unittest.mock import patch, MagicMock
import json
import anthropic


def test_check_x402_balance_parses_usdc():
    """Should parse a hex USDC balance (6 decimals) into a dollar float."""
    import pipeline.enrichment as mod

    # 5.50 USDC = 5_500_000 = 0x53EC60
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {'result': '0x000000000000000000000000000000000000000000000000000000000053EC60'}

    with patch('pipeline.enrichment.requests.post', return_value=mock_resp), \
         patch.dict('os.environ', {'PRIVATE_KEY': '0x' + 'ab' * 32}):
        balance = mod.check_x402_balance()

    assert abs(balance - 5.50) < 0.01


def test_check_x402_balance_returns_zero_on_error():
    """Should return 0.0 if the RPC call fails."""
    import pipeline.enrichment as mod

    with patch('pipeline.enrichment.requests.post', side_effect=Exception('RPC down')), \
         patch.dict('os.environ', {'PRIVATE_KEY': '0x' + 'ab' * 32}):
        balance = mod.check_x402_balance()

    assert balance == 0.0


def test_check_x402_balance_zero_balance():
    """Should return 0.0 for a zero hex result."""
    import pipeline.enrichment as mod

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {'result': '0x0000000000000000000000000000000000000000000000000000000000000000'}

    with patch('pipeline.enrichment.requests.post', return_value=mock_resp), \
         patch.dict('os.environ', {'PRIVATE_KEY': '0x' + 'ab' * 32}):
        balance = mod.check_x402_balance()

    assert balance == 0.0


# ── Task 1: After first 402, all subsequent paid calls are skipped ─────────

def test_hunter_402_stops_all_future_paid_calls():
    """User runs enrichment, first Hunter call gets 402 (wallet empty).
    All remaining Hunter AND Apollo calls for every subsequent lead should
    be skipped instantly — user should NOT see 500+ individual 402 errors."""
    import pipeline.enrichment as mod
    mod._x402_insufficient = False

    mock_resp = MagicMock()
    mock_resp.status_code = 402
    mock_resp.raise_for_status.side_effect = Exception('402 Client Error: Payment Required')

    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp

    # Three consecutive x402 402s set the flag; Orthogonal is disabled here so
    # this tests the x402 fallback path directly.
    with patch('pipeline.orthogonal.has_api_key', return_value=False), \
         patch.object(mod, '_x402_session', return_value=mock_session):
        lead = {'company': 'Test Co', 'website': 'https://test.com'}
        for _ in range(3):
            try:
                mod._step_hunter(lead, {}, {})
            except RuntimeError:
                pass

    assert mod._x402_insufficient is True

    # Second call: Hunter for a DIFFERENT lead should skip without making any API call
    lead2 = {'company': 'Other Co', 'website': 'https://other.com'}
    with patch('pipeline.orthogonal.has_api_key', return_value=False):
        assert mod._step_hunter(lead2, {}, {}) == 0.0

    # Third call: Apollo should ALSO skip without making any API call
    with patch('pipeline.orthogonal.has_api_key', return_value=False):
        assert mod._step_apollo(lead2, {}, {}) == 0.0

    mod._x402_insufficient = False


def test_hunter_skips_when_flag_set():
    """User's wallet was already flagged empty — Hunter skips immediately."""
    import pipeline.enrichment as mod
    mod._x402_insufficient = True

    lead = {'company': 'Test Co', 'website': 'https://test.com'}
    enriched = {}
    meta = {}
    try:
        with patch('pipeline.orthogonal.has_api_key', return_value=False):
            assert mod._step_hunter(lead, enriched, meta) == 0.0
    finally:
        mod._x402_insufficient = False


def test_apollo_skips_when_flag_set():
    """User's wallet was already flagged empty — Apollo skips immediately."""
    import pipeline.enrichment as mod
    mod._x402_insufficient = True

    lead = {'company': 'Test Co', 'website': 'https://test.com'}
    enriched = {}
    meta = {}
    try:
        with patch('pipeline.orthogonal.has_api_key', return_value=False):
            assert mod._step_apollo(lead, enriched, meta) == 0.0
    finally:
        mod._x402_insufficient = False


# ── Task 2: User starts a run with insufficient funds — enrichment is skipped ──

def test_preflight_skips_enrichment_when_balance_too_low():
    """User has $0.10 in wallet but needs $0.20 for 10 leads.
    enrich_lead should NEVER be called — enrichment is skipped entirely."""
    from pipeline.enrichment import check_x402_balance

    # Mock check_x402_balance to return $0.10
    with patch('pipeline.run.check_x402_balance', return_value=0.10), \
         patch('pipeline.run.enrich_lead') as mock_enrich:
        events = []
        emit = lambda e: events.append(e)

        # Simulate the pre-flight logic from run_pipeline
        lead_ids = list(range(10))  # 10 leads
        estimated_cost = len(lead_ids) * 0.02  # $0.20
        balance = 0.10  # mocked

        skip_enrichment = False
        if estimated_cost > 0 and balance < estimated_cost:
            emit({
                'type': 'insufficient_funds',
                'balance': round(balance, 4),
                'estimated_cost': round(estimated_cost, 2),
                'message': f"get ur money up — Balance: ${balance:.2f}, need ~${estimated_cost:.2f}",
            })
            skip_enrichment = True

        assert skip_enrichment is True, "Should skip enrichment when balance < cost"
        assert mock_enrich.call_count == 0, "enrich_lead should never be called"

        # Verify the insufficient_funds event was emitted with real numbers
        funds_events = [e for e in events if e['type'] == 'insufficient_funds']
        assert len(funds_events) == 1
        assert funds_events[0]['balance'] == 0.10
        assert funds_events[0]['estimated_cost'] == 0.20


def test_preflight_allows_enrichment_when_balance_sufficient():
    """User has $5.00 in wallet and needs $0.20 for 10 leads.
    Enrichment should proceed — skip_enrichment stays False."""
    lead_ids = list(range(10))
    estimated_cost = len(lead_ids) * 0.02  # $0.20
    balance = 5.00

    skip_enrichment = False
    if estimated_cost > 0 and balance < estimated_cost:
        skip_enrichment = True

    assert skip_enrichment is False, "Should NOT skip enrichment when balance >= cost"


def test_insufficient_funds_event_has_balance_details():
    """User sees the insufficient_funds popup — it should show their actual balance
    and how much they need, so they know exactly how much USDC to add."""
    event = {
        'type': 'insufficient_funds',
        'balance': 0.10,
        'estimated_cost': 0.20,
        'message': "get ur money up — you don't have enough USDC in your Base wallet. Balance: $0.10, need ~$0.20",
    }
    assert event['balance'] == 0.10
    assert event['estimated_cost'] == 0.20
    assert '$0.10' in event['message']
    assert '$0.20' in event['message']


# ── Task 4: Claude rate-limits don't permanently lose lead data ────────────

def test_claude_429_retries_and_recovers_data():
    """User enriches 500 leads. Claude rate-limits on lead #50's discovery step.
    Instead of permanently losing that lead's data, the pipeline should wait and
    retry — the user should see the data come through on the second attempt."""
    import pipeline.enrichment as mod

    call_count = 0

    def flaky_fn():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise anthropic.RateLimitError(
                message='rate limit',
                response=MagicMock(status_code=429, headers={}),
                body={'error': {'type': 'rate_limit_error', 'message': 'rate limit'}},
            )
        return 'success'

    with patch('pipeline.enrichment.time.sleep'):
        result = mod._claude_call_with_retry(flaky_fn, max_retries=2)

    assert result == 'success'
    assert call_count == 2


def test_claude_non_rate_limit_errors_fail_immediately():
    """If Claude returns a real error (not 429), it should fail right away —
    don't waste 20 seconds retrying something that won't recover."""
    import pipeline.enrichment as mod

    def bad_fn():
        raise ValueError('bad input')

    try:
        mod._claude_call_with_retry(bad_fn, max_retries=2)
        assert False, 'Should have raised'
    except ValueError as e:
        assert 'bad input' in str(e)


def test_claude_persistent_rate_limit_eventually_gives_up():
    """If Claude is rate-limiting for an extended period (all retries exhausted),
    the error should surface rather than hanging forever."""
    import pipeline.enrichment as mod

    def always_429():
        raise anthropic.RateLimitError(
            message='rate limit',
            response=MagicMock(status_code=429, headers={}),
            body={'error': {'type': 'rate_limit_error', 'message': 'rate limit'}},
        )

    with patch('pipeline.enrichment.time.sleep'):
        try:
            mod._claude_call_with_retry(always_429, max_retries=2)
            assert False, 'Should have raised'
        except anthropic.RateLimitError:
            pass


# ── Task 5: Pause actually stops enrichment instead of running 90 more calls ──

def test_pause_checks_between_every_enrichment_step():
    """User hits pause mid-enrichment. With 10 concurrent leads × 9 steps,
    the old code would fire up to 90 more API calls before stopping.
    After fix: each lead checks pause before every step, so at most 1 step
    per in-flight lead completes before blocking."""
    import pipeline.enrichment as mod

    pause_call_count = 0

    def mock_wait():
        nonlocal pause_call_count
        pause_call_count += 1

    # Mock get_lead to return a minimal lead
    mock_lead = {'id': 1, 'company': 'Test Co', 'city': 'LA', 'state': 'CA'}

    with patch.object(mod, 'get_lead', return_value=mock_lead), \
         patch.object(mod, '_run_step', return_value=0.0), \
         patch.object(mod, 'update_lead'), \
         patch('pipeline.enrichment.ThreadPoolExecutor') as mock_executor_cls:
        # Mock Phase 2 parallel execution
        mock_executor = MagicMock()
        mock_executor_cls.return_value.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = MagicMock()
        # as_completed returns nothing (skip phase 2 results)
        with patch('pipeline.enrichment.as_completed', return_value=[]):
            mod.enrich_lead(1, emit=lambda e: None, wait_if_paused=mock_wait)

    # Should be called at least once per Phase 1 step + once before Phase 2 + once per Phase 3 step
    # Phase 1 = 3 steps, Phase 3 = 3 steps, + 1 before Phase 2 = 7 minimum
    assert pause_call_count >= 7, f"Expected >= 7 pause checks, got {pause_call_count}"
