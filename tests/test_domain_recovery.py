"""Tests for verified company website recovery."""

from unittest.mock import MagicMock, patch


def test_filter_rejects_social_directory_and_marketplace_domains():
    import pipeline.google_search as mod

    candidates = [
        'https://www.facebook.com/acme',
        'https://linkedin.com/company/acme',
        'https://maps.google.com/?cid=1',
        'https://www.yelp.com/biz/acme',
        'https://www.amazon.com/acme-store',
        'https://acme-industrial.com/about',
    ]

    assert mod._filter_business_candidates(candidates) == ['https://acme-industrial.com/about']


def test_filter_normalizes_bare_domain_candidate():
    import pipeline.google_search as mod

    assert mod._filter_business_candidates(['acme-industrial.com']) == ['https://acme-industrial.com']


def test_query_expands_linkedin_company_slug():
    import pipeline.google_search as mod

    query = mod._query(
        'KWI',
        address='18655 Bishop Avenue, Carson, CA',
        city='Carson',
        state='CA',
        zipcode='90746',
        company_linkedin='https://www.linkedin.com/company/kw-international/',
    )

    assert 'kw international' in query.lower()
    assert '18655 Bishop Avenue' in query
    assert '90746' in query


def test_linkedin_company_terms_clean_messy_slug():
    import pipeline.google_search as mod

    terms = mod._linkedin_company_terms("https://www.linkedin.com/company/raquel's-cash-n'-carry-llc-")

    assert terms == ['raquel', 'cash', 'carry']


def test_openmart_record_urls_reads_nested_content():
    import pipeline.google_search as mod

    urls = mod._record_urls({
        'content': {
            'website_url': 'https://www.kwinternational.com/',
            'from_sources': {
                'GOOGLE_MAP': {'raw_associated_website': 'https://fallback.example'}
            },
        }
    })

    assert 'https://www.kwinternational.com/' in urls


def test_openmart_returns_verified_canonical_url():
    import pipeline.google_search as mod

    with patch.object(mod, '_openmart_search', return_value=['https://acme-industrial.com/about']), \
         patch.object(mod, '_fetch_page_text', return_value='Acme Industrial serves Downey.'):
        url, provider, rejected = mod.find_company_website_with_provider('Acme Industrial', city='Downey', state='CA')

    assert url == 'https://acme-industrial.com'
    assert provider == 'openmart'
    assert rejected == []


def test_cse_fallback_returns_verified_canonical_url():
    import pipeline.google_search as mod

    with patch.dict('os.environ', {'GOOGLE_CSE_API_KEY': 'k', 'GOOGLE_CSE_ID': 'cx'}, clear=False), \
         patch.object(mod, '_openmart_search', return_value=[]), \
         patch.object(mod, '_cse_search', return_value=['https://acme-industrial.com/about']), \
         patch.object(mod, '_fetch_page_text', return_value='Acme Industrial serves Downey.'):
        url = mod.find_company_website('Acme Industrial', city='Downey', state='CA')

    assert url == 'https://acme-industrial.com'


def test_orth_skill_preferred_when_configured():
    import pipeline.google_search as mod

    fake_orth = MagicMock(return_value=['https://acme.com'])
    fake_cse = MagicMock(return_value=['https://wrong.com'])
    with patch.dict('os.environ', {'ORTH_FIND_WEBSITE_PATH': '/search/web'}, clear=False), \
         patch.object(mod, '_openmart_search', return_value=[]), \
         patch.object(mod, '_orth_search', fake_orth), \
         patch.object(mod, '_cse_search', fake_cse), \
         patch.object(mod, '_verify_candidate', return_value=True):
        url = mod.find_company_website('Acme')

    assert url == 'https://acme.com'
    fake_orth.assert_called_once()
    fake_cse.assert_not_called()


def test_unverified_candidate_is_rejected():
    import pipeline.google_search as mod

    with patch.dict('os.environ', {'GOOGLE_CSE_API_KEY': 'k', 'GOOGLE_CSE_ID': 'cx'}, clear=False), \
         patch.object(mod, '_openmart_search', return_value=[]), \
         patch.object(mod, '_cse_search', return_value=['https://random-site.com']), \
         patch.object(mod, '_fetch_page_text', return_value='nothing relevant here'):
        url = mod.find_company_website('Acme Industrial', address='123 Main St')

    assert url is None


def test_rejected_candidates_are_returned_for_diagnostics():
    import pipeline.google_search as mod

    with patch.object(mod, '_openmart_search', return_value=['https://random-site.com']), \
         patch.object(mod, '_cse_search', return_value=[]), \
         patch.object(mod, '_guess_domain_candidates', return_value=[]), \
         patch.object(mod, '_fetch_page_text', return_value='nothing relevant here'):
        url, provider, rejected = mod.find_company_website_with_provider('Acme Industrial', address='123 Main St')

    assert url is None
    assert provider is None
    assert rejected == [{'provider': 'openmart', 'url': 'https://random-site.com', 'reason': 'verification_failed'}]


def test_city_or_zip_plus_company_token_can_verify_candidate():
    import pipeline.google_search as mod

    with patch.object(mod, '_fetch_page_text', return_value='Acme parts warehouse in Downey CA 90241'):
        assert mod._verify_candidate(
            'https://www.acmeparts.com',
            'Acme Industrial',
            city='Downey',
            state='CA',
            zipcode='90241',
        ) is True


def test_short_acronym_candidate_needs_stronger_verification():
    import pipeline.google_search as mod

    with patch.object(mod, '_fetch_page_text', return_value='KWI retail software platform'):
        assert mod._verify_candidate(
            'https://www.kwi.com',
            'KWI',
            address='18655 Bishop Avenue, Carson, CA',
            company_linkedin='https://www.linkedin.com/company/kw-international/',
        ) is False


def test_linkedin_alias_and_address_can_verify_acronym_candidate():
    import pipeline.google_search as mod

    page = 'KW International Inc. 18655 Bishop Avenue Carson CA logistics service'
    with patch.object(mod, '_fetch_page_text', return_value=page):
        assert mod._verify_candidate(
            'https://www.kwinternational.com',
            'KWI',
            address='18655 Bishop Avenue, Carson, CA',
            city='Carson',
            state='CA',
            company_linkedin='https://www.linkedin.com/company/kw-international/',
        ) is True


def test_step_domain_recovery_sets_website_with_source():
    import pipeline.enrichment as mod

    enriched = {}
    meta = {}
    with patch('pipeline.google_search.find_company_website_with_provider', return_value=('https://acme.com', 'openmart', [])):
        cost = mod._step_domain_recovery(
            {'company': 'Acme', 'address': '123 Main St', 'city': 'LA', 'state': 'CA'},
            enriched,
            meta,
        )

    assert cost == 0.0
    assert enriched['website'] == 'https://acme.com'
    assert meta['website'] == {'source': 'domain_recovery', 'provider': 'openmart'}
