import os
from dotenv import load_dotenv

from pipeline.config import SEARCH_PROVIDER
from pipeline.google_places import get_place_details, search_text
from pipeline.openmart import search_business_records

load_dotenv()

API_PAGE_SIZE = 50  # internal batch size per Openmart call


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
        results = search_business_records(
            f'{query} in {city}',
            page_size=page_size,
            min_rating=min_rating,
            min_reviews=min_reviews,
            cursor=cursor,
        )
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
