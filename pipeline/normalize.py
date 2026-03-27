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
        'email':           extract_email(raw),
        'phone':           phones[0] if phones else None,
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
