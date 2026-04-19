import math
import json
from pipeline.config import LA_LAT, LA_LNG, GEO_RADIUS_MILES, TARGET_CITIES


# ---------------------------------------------------------------------------
# GEO FILTER
# ---------------------------------------------------------------------------

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def is_socal_target(raw):
    # Layer 1: fast state check
    state = (raw.get('state') or '').strip()
    if state and state not in ('California', 'CA'):
        return False, f'wrong state: {state}'

    # Layer 2: coordinate check (preferred path)
    lat = raw.get('latitude')
    lng = raw.get('longitude')
    if lat and lng:
        dist = haversine_miles(LA_LAT, LA_LNG, lat, lng)
        if dist <= GEO_RADIUS_MILES:
            return True, round(dist, 1)
        return False, f'too far: {round(dist, 1)} miles'

    # Layer 3: city name fallback
    city = (raw.get('city') or '').strip().lower()
    if city in TARGET_CITIES:
        return True, None
    return False, 'no location data'


# ---------------------------------------------------------------------------
# FIELD EXTRACTION
# ---------------------------------------------------------------------------

def extract_owner(raw):
    staffs = raw.get('staffs') or []
    for s in staffs:
        role = (s.get('role') or '').lower()
        if any(r in role for r in ['owner', 'founder', 'president', 'ceo']):
            return s.get('name')
    return None


def extract_email(raw):
    FILLER_DOMAINS = {
        'godaddy.com', 'wix.com', 'squarespace.com',
        'wordpress.com', 'example.com',
    }
    emails = raw.get('business_emails') or []
    for email in emails:
        if '@' in email:
            domain = email.split('@')[1].lower()
            if domain not in FILLER_DOMAINS:
                return email.lower().strip()
    return None


def extract_place_id(raw):
    source_id = raw.get('source_id') or ''
    if source_id.startswith('GOOGLE_MAP@'):
        return source_id.replace('GOOGLE_MAP@', '')
    return None


# ---------------------------------------------------------------------------
# NORMALIZE
# ---------------------------------------------------------------------------

def normalize_lead(record, industry):
    if record.get('source') == 'google_places':
        return normalize_google_place(record, industry)

    # Openmart wraps fields under 'content'
    raw = record.get('content') or record

    # Step 1: geo filter
    in_target, dist_or_reason = is_socal_target(raw)
    if not in_target:
        return None

    # Step 2: require dedup key
    place_id = extract_place_id(raw)
    if not place_id:
        return None

    # Step 3: require company name
    company = (raw.get('business_name') or raw.get('store_name') or '').strip()
    if not company:
        return None

    # Step 4: map all fields
    phones = raw.get('store_phones') or raw.get('business_phones') or []
    return {
        'company':         company,
        'owner_name':      extract_owner(raw),
        'company_email':   extract_email(raw),
        'company_phone':   phones[0] if phones else None,
        'address':         raw.get('street_address'),
        'city':            raw.get('city'),
        'state':           raw.get('state') or 'CA',
        'zipcode':         raw.get('zipcode'),
        'website':         raw.get('website_url') or raw.get('root_domain'),
        'industry':        industry,
        'google_place_id': place_id,
        'rating':          raw.get('google_rating'),
        'review_count':    raw.get('google_reviews_count'),
        'ownership_type':  raw.get('ownership_type') or None,
        'distance_miles':  dist_or_reason if isinstance(dist_or_reason, float) else None,
        'latitude':        raw.get('latitude'),
        'longitude':       raw.get('longitude'),
        'openmart_id':     raw.get('store_id'),
        'status':          'new',
        'source':          'openmart',
        'raw_data':        json.dumps(record),
    }


def normalize_google_place(record, industry):
    details = record.get('details') or record.get('place') or {}
    location = details.get('location') or {}
    display_name = details.get('displayName') or {}
    primary_type = details.get('primaryTypeDisplayName') or {}
    address = details.get('formattedAddress') or ''
    city = None
    state = None
    zipcode = None

    if address:
        parts = [part.strip() for part in address.split(',')]
        if len(parts) >= 3:
            city = parts[-3]
            state_zip = parts[-2].split()
            if state_zip:
                state = state_zip[0]
            if len(state_zip) > 1:
                zipcode = state_zip[1]

    raw = {
        'company': (display_name.get('text') or '').strip(),
        'address': address,
        'city': city,
        'state': state or 'CA',
        'zipcode': zipcode,
        'website': details.get('websiteUri'),
        'phone': details.get('nationalPhoneNumber'),
        'industry': industry,
        'google_place_id': details.get('id'),
        'rating': details.get('rating'),
        'review_count': details.get('userRatingCount'),
        'latitude': location.get('latitude'),
        'longitude': location.get('longitude'),
        'google_maps_url': details.get('googleMapsUri'),
        'source': 'google_places',
        'raw_data': json.dumps(record),
        'primary_type': primary_type.get('text'),
    }

    in_target, dist_or_reason = is_socal_target({
        'state': raw.get('state'),
        'city': raw.get('city'),
        'latitude': raw.get('latitude'),
        'longitude': raw.get('longitude'),
    })
    if not in_target:
        return None

    if not raw['company'] or not raw['google_place_id']:
        return None

    return {
        'company': raw['company'],
        'owner_name': None,
        'company_email': None,
        'company_phone': raw['phone'],
        'address': raw['address'],
        'city': raw['city'],
        'state': raw['state'],
        'zipcode': raw['zipcode'],
        'website': raw['website'],
        'industry': raw['industry'],
        'google_place_id': raw['google_place_id'],
        'rating': raw['rating'],
        'review_count': raw['review_count'],
        'ownership_type': None,
        'distance_miles': dist_or_reason if isinstance(dist_or_reason, float) else None,
        'latitude': raw['latitude'],
        'longitude': raw['longitude'],
        'openmart_id': None,
        'google_maps_url': raw['google_maps_url'],
        'status': 'new',
        'source': 'google_places',
        'raw_data': raw['raw_data'],
    }
