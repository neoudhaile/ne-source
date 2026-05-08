from unittest.mock import MagicMock, patch

import pytest
import requests


def test_run_api_posts_to_orthogonal_and_unwraps_success_data():
    import pipeline.orthogonal as mod

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {'success': True, 'data': {'ok': True}}

    with patch.dict('os.environ', {'ORTHOGONAL_API_KEY': 'orth_test'}, clear=False), \
         patch.object(mod.requests, 'post', return_value=resp) as post:
        result = mod.run_api('hunter', '/v2/domain-search', query={'domain': 'acme.com'})

    assert result == {'ok': True}
    body = post.call_args.kwargs['json']
    assert body['api'] == 'hunter'
    assert body['path'] == '/v2/domain-search'
    assert body['query'] == {'domain': 'acme.com'}


def test_run_api_stringifies_query_values():
    import pipeline.orthogonal as mod

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {'success': True, 'data': {}}

    with patch.dict('os.environ', {'ORTHOGONAL_API_KEY': 'orth_test'}, clear=False), \
         patch.object(mod.requests, 'post', return_value=resp) as post:
        mod.run_api('hunter', '/v2/domain-search', query={'domain': 'acme.com', 'limit': 5})

    assert post.call_args.kwargs['json']['query'] == {'domain': 'acme.com', 'limit': '5'}


def test_call_with_fallback_uses_fallback_when_orthogonal_fails():
    import pipeline.orthogonal as mod

    with patch.dict('os.environ', {'ORTHOGONAL_API_KEY': 'orth_test'}, clear=False), \
         patch.object(mod, 'run_api', side_effect=RuntimeError('down')):
        result = mod.call_with_fallback(
            'apollo',
            '/api/v1/people/match',
            fallback=lambda: {'person': {'name': 'Jane'}},
        )

    assert result == {'person': {'name': 'Jane'}}


def test_call_with_fallback_does_not_mask_orthogonal_4xx():
    import pipeline.orthogonal as mod

    response = MagicMock()
    response.status_code = 400
    error = requests.exceptions.HTTPError('bad request', response=response)

    with patch.dict('os.environ', {'ORTHOGONAL_API_KEY': 'orth_test'}, clear=False), \
         patch.object(mod, 'run_api', side_effect=error):
        with pytest.raises(requests.exceptions.HTTPError):
            mod.call_with_fallback(
                'hunter',
                '/v2/email-finder',
                fallback=lambda: {'masked': True},
            )


def test_call_with_fallback_does_not_fallback_on_timeout():
    import pipeline.orthogonal as mod

    with patch.dict('os.environ', {'ORTHOGONAL_API_KEY': 'orth_test'}, clear=False), \
         patch.object(mod, 'run_api', side_effect=requests.exceptions.Timeout('slow')):
        with pytest.raises(requests.exceptions.Timeout):
            mod.call_with_fallback(
                'sixtyfour',
                '/enrich-company',
                fallback=lambda: {'masked': True},
            )
