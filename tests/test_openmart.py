from unittest.mock import patch


def test_openmart_uses_orthogonal_client_before_x402():
    import pipeline.openmart as mod

    with patch.object(mod, 'call_with_fallback', return_value={'data': [{'website': 'https://acme.com'}]}) as call, \
         patch.object(mod, '_call_openmart_x402') as x402:
        records = mod.search_business_records('Acme LA')

    assert records == [{'website': 'https://acme.com'}]
    call.assert_called_once()
    assert call.call_args.args[:2] == ('openmart', '/api/v1/search')
    x402.assert_not_called()


def test_openmart_passes_x402_fallback_to_orthogonal_client():
    import pipeline.openmart as mod

    captured = {}

    def fake_call(*args, **kwargs):
        captured.update(kwargs)
        return {'data': {'owner_name': 'Jane Smith'}}

    with patch.object(mod, 'call_with_fallback', side_effect=fake_call):
        result = mod.enrich_company(company_website='https://acme.com')

    assert result == {'owner_name': 'Jane Smith'}
    assert callable(captured['fallback'])


def test_openmart_fallback_uses_x402_route():
    import pipeline.openmart as mod

    def fake_call(*args, **kwargs):
        return kwargs['fallback']()

    with patch.object(mod, 'call_with_fallback', side_effect=fake_call), \
         patch.object(mod, '_call_openmart_x402', return_value={'data': {'owner_name': 'Jane Smith'}}) as x402:
        result = mod.enrich_company(company_website='https://acme.com')

    assert result == {'owner_name': 'Jane Smith'}
    x402.assert_called_once()
