import os
import requests
import json
from eth_account import Account
from x402 import x402ClientSync
from x402.mechanisms.evm.exact.v1.client import ExactEvmSchemeV1
from x402.http.clients.requests import x402_http_adapter
from dotenv import load_dotenv

load_dotenv()

account = Account.from_key(os.getenv('PRIVATE_KEY'))
client = x402ClientSync()
client.register_v1('base', ExactEvmSchemeV1(signer=account))
session = requests.Session()
session.mount('https://', x402_http_adapter(client))


API_PAGE_SIZE = 50  # internal batch size per Openmart call


def search_businesses(query, city, page_size, min_rating, min_reviews, cursor=None):
    """
    Single Openmart API call. Returns (results_list, next_cursor).
    next_cursor is None when there are no more pages.
    """
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
