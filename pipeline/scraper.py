import os
import requests
from eth_account import Account
from x402 import x402ClientSync
from x402.mechanisms.evm.exact.v1.client import ExactEvmSchemeV1
from x402.http.clients.requests import x402_http_adapter
from dotenv import load_dotenv

from pipeline.config import SEARCH_PROVIDER
from pipeline.google_places import get_place_details, search_text

load_dotenv()

API_PAGE_SIZE = 50  # internal batch size per Openmart call


def _x402_session() -> requests.Session:
    """Create a fresh x402-backed session per call to avoid shared-session issues."""
    account = Account.from_key(os.getenv('PRIVATE_KEY'))
    client = x402ClientSync()
    client.register_v1('base', ExactEvmSchemeV1(signer=account))
    session = requests.Session()
    session.mount('https://', x402_http_adapter(client))
    return session


def search_businesses(query, city, page_size, min_rating, min_reviews, cursor=None):
    """
    Provider-backed search call. Returns (results_list, next_cursor).
    next_cursor is None when there are no more pages.
    """
    if SEARCH_PROVIDER == 'google_places':
        return _search_google_places(query, city, page_size, cursor)
    return _search_openmart(query, city, page_size, min_rating, min_reviews, cursor)


def _search_google_places(query, city, page_size, cursor=None):
    search_query = f'{query} in {city}'
    print(f'  Searching Google Places: {search_query}...')
    try:
        places, next_token = search_text(search_query, page_size=page_size, page_token=cursor)
        results = []
        for place in places:
            place_id = place.get('id')
            if not place_id:
                continue
            try:
                details = get_place_details(place_id)
            except Exception:
                details = place
            results.append({
                'source': 'google_places',
                'place': place,
                'details': details,
            })
        print(f'  Found {len(results)} Google Places results')
        return results, next_token
    except Exception as e:
        print(f'  Google Places error: {e}')
        return [], None


def _search_openmart(query, city, page_size, min_rating, min_reviews, cursor=None):
    print(f'  Searching: {query} in {city}...')
    try:
        payload = {
            'query': f'{query} in {city}',
            'page_size': page_size,
            'min_rating': min_rating,
            'min_reviews': min_reviews,
        }
        if cursor is not None:
            payload['cursor'] = cursor

        with _x402_session() as session:
            response = session.post(
                'https://x402.orth.sh/openmart/api/v1/search',
                json=payload,
            )
        response.raise_for_status()
        body = response.json()
        results = body if isinstance(body, list) else body.get('data', [])
        print(f'  Found {len(results)} results')

        # Extract cursor from last result for pagination
        next_cursor = None
        if results and len(results) >= page_size:
            last = results[-1]
            c = last.get('cursor')
            if c:
                next_cursor = c

        return results, next_cursor
    except Exception as e:
        print(f'  Error: {e}')
        return [], None
