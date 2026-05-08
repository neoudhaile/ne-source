import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline.scraper import search_businesses
from pipeline.normalize import normalize_lead

results, _next_cursor = search_businesses(
    query='HVAC repair',
    city='Los Angeles, CA',
    page_size=10,
    min_rating=3.5,
    min_reviews=5
)

for record in results:
    raw = record.get('content') or record
    company = raw.get('business_name') or raw.get('store_name') or 'Unknown'
    lead = normalize_lead(record, 'HVAC repair')
    if lead:
        print(f'  PASS {company} -- {lead["distance_miles"]} miles from LA')
    else:
        print(f'  SKIP {company} -- filtered out')
