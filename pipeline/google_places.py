import math
import os
import re
from difflib import SequenceMatcher

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


def _normalize_text(value: str | None) -> str:
    text = str(value or '').lower()
    text = re.sub(r'&', ' and ', text)
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    stopwords = {
        'inc', 'llc', 'ltd', 'co', 'company', 'corp', 'corporation',
    }
    tokens = [token for token in text.split() if token and token not in stopwords]
    return ' '.join(tokens)


def _street_number(value: str | None) -> str | None:
    match = re.match(r'^\s*(\d+)\b', str(value or ''))
    return match.group(1) if match else None


def _street_tokens(value: str | None) -> set[str]:
    text = _normalize_text((str(value or '')).split(',', 1)[0])
    stopwords = {
        'street', 'st', 'avenue', 'ave', 'road', 'rd', 'boulevard', 'blvd',
        'drive', 'dr', 'lane', 'ln', 'way', 'place', 'pl', 'court', 'ct',
        'parkway', 'pkwy', 'suite', 'ste',
    }
    return {token for token in text.split() if token not in stopwords}


def _similarity(left: str | None, right: str | None) -> float:
    left_norm = _normalize_text(left)
    right_norm = _normalize_text(right)
    if not left_norm or not right_norm:
        return 0.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def _address_score(address: str | None, formatted_address: str | None) -> float:
    if not address or not formatted_address:
        return 0.0
    address_num = _street_number(address)
    candidate_num = _street_number(formatted_address)
    if address_num and candidate_num and address_num != candidate_num:
        return 0.0
    source_tokens = _street_tokens(address)
    candidate_tokens = _street_tokens(formatted_address)
    if not source_tokens or not candidate_tokens:
        return 0.0
    overlap = len(source_tokens & candidate_tokens) / max(len(source_tokens), 1)
    base = 0.4 if (address_num and candidate_num and address_num == candidate_num) else 0.2
    return min(1.0, base + overlap * 0.8)


def _location_score(city: str | None, state: str | None, formatted_address: str | None) -> float:
    formatted = str(formatted_address or '').lower()
    score = 0.0
    if city and str(city).lower() in formatted:
        score += 0.6
    if state and str(state).lower() in formatted:
        score += 0.4
    return score


def _candidate_score(
    company: str,
    candidate_name: str | None,
    formatted_address: str | None,
    address: str | None = None,
    city: str | None = None,
    state: str | None = None,
) -> float:
    name_score = _similarity(company, candidate_name)
    if address:
        address_score = _address_score(address, formatted_address)
        if name_score < 0.68 or address_score < 0.55:
            return 0.0
        return name_score * 0.7 + address_score * 0.3

    location_score = _location_score(city, state, formatted_address)
    if name_score < 0.82 or location_score < 0.4:
        return 0.0
    return name_score * 0.8 + location_score * 0.2


def _search_for_enrichment(query: str, page_size: int = 5) -> list[dict]:
    """Search Google Places without minRating or locationRestriction filters.

    Used by find_place() during enrichment — we already know the company exists
    at a specific address, so scraper-oriented filters would wrongly exclude it.
    """
    payload = {
        'textQuery': query,
        'pageSize': min(page_size, 20),
    }
    response = requests.post(
        SEARCH_URL,
        headers=_headers(SEARCH_FIELDS),
        json=payload,
        timeout=FAST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json().get('places', [])


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

    places = _search_for_enrichment(', '.join(part for part in parts if part))
    if not places:
        return None

    best_place = None
    best_score = 0.0
    for place in places:
        candidate_name = (place.get('displayName') or {}).get('text')
        candidate_address = place.get('formattedAddress')
        score = _candidate_score(
            company=company,
            candidate_name=candidate_name,
            formatted_address=candidate_address,
            address=address,
            city=city,
            state=state,
        )
        if score > best_score:
            best_score = score
            best_place = place

    if best_place is None or best_score < 0.72:
        return None

    place_id = best_place.get('id')
    if not place_id:
        return best_place
    try:
        return get_place_details(place_id)
    except Exception:
        return best_place
