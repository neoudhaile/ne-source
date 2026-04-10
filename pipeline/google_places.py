import math
import os

import requests
from dotenv import load_dotenv

from pipeline.config import FAST_TIMEOUT, GEO_RADIUS_MILES, LA_LAT, LA_LNG, MIN_RATING

load_dotenv()

SEARCH_URL = 'https://places.googleapis.com/v1/places:searchText'
DETAILS_URL = 'https://places.googleapis.com/v1/places/{place_id}'

SEARCH_FIELDS = [
    'places.id',
    'places.displayName',
    'places.formattedAddress',
    'places.location',
    'places.primaryTypeDisplayName',
    'places.rating',
    'places.userRatingCount',
    'places.businessStatus',
]

DETAIL_FIELDS = [
    'id',
    'displayName',
    'formattedAddress',
    'location',
    'primaryTypeDisplayName',
    'rating',
    'userRatingCount',
    'businessStatus',
    'nationalPhoneNumber',
    'websiteUri',
    'googleMapsUri',
]


def _api_key() -> str:
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        raise RuntimeError('GOOGLE_MAPS_API_KEY is not set')
    return api_key


def _headers(field_mask: list[str]) -> dict:
    return {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': _api_key(),
        'X-Goog-FieldMask': ','.join(field_mask),
    }


def _bounding_box(lat: float, lng: float, radius_miles: float) -> dict:
    lat_delta = radius_miles / 69.0
    lng_delta = radius_miles / (math.cos(math.radians(lat)) * 69.0)
    return {
        'low': {
            'latitude': lat - lat_delta,
            'longitude': lng - lng_delta,
        },
        'high': {
            'latitude': lat + lat_delta,
            'longitude': lng + lng_delta,
        },
    }


def search_text(query: str, page_size: int = 20, page_token: str | None = None) -> tuple[list[dict], str | None]:
    payload = {
        'textQuery': query,
        'pageSize': min(page_size, 20),
        'minRating': MIN_RATING,
        'locationRestriction': {
            'rectangle': _bounding_box(LA_LAT, LA_LNG, GEO_RADIUS_MILES),
        },
    }
    if page_token:
        payload['pageToken'] = page_token

    response = requests.post(
        SEARCH_URL,
        headers=_headers(SEARCH_FIELDS),
        json=payload,
        timeout=FAST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    return data.get('places', []), data.get('nextPageToken')


def get_place_details(place_id: str) -> dict:
    response = requests.get(
        DETAILS_URL.format(place_id=place_id),
        headers=_headers(DETAIL_FIELDS),
        timeout=FAST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def find_place(company: str, address: str | None = None, city: str | None = None, state: str | None = None) -> dict | None:
    """
    Best-effort place match for CSV/imported leads using business identity text.
    """
    parts = [company]
    if address:
        parts.append(address)
    else:
        location = ', '.join(part for part in [city, state] if part)
        if location:
            parts.append(location)

    places, _ = search_text(', '.join(part for part in parts if part), page_size=1)
    if not places:
        return None

    place_id = places[0].get('id')
    if not place_id:
        return places[0]
    try:
        return get_place_details(place_id)
    except Exception:
        return places[0]
